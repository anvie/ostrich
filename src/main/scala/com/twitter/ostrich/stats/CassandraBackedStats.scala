package com.twitter.ostrich.stats

import com.twitter.ostrich.admin.{PeriodicBackgroundProcess, AdminHttpService, StatsReporterFactory}
import com.twitter.util.{Time, Duration}
import com.netflix.astyanax.{Cluster, Keyspace, AstyanaxContext}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{ConnectionPoolType, ConnectionPoolConfigurationImpl, Slf4jConnectionPoolMonitorImpl}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import scala.collection.immutable.HashMap
import com.twitter.logging.Logger
import org.apache.cassandra.utils.UUIDGen
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar, UUID, Date}
import org.apache.commons.lang.time.DateUtils
import com.netflix.astyanax.retry.BoundedExponentialBackoff
import com.netflix.astyanax.model.ConsistencyLevel

/**
 * Author: robin
 * Date: 7/24/13
 * Time: 11:10 AM
 *
 */


/**
 * Create cassandra backed stats collection.
// * @param clusterName cassandra cluster name.
// * @param keyspaceName cassandra keyspace name.
// * @param seeds cassandra seeds.
 * @param period period.
 * @param TTL time to live.
 */
class CassandraBackedStatsFactory(val keyspace:Keyspace, val TTL:Int,
                                  val period:Duration) extends StatsReporterFactory {
  def apply(collection: StatsCollection, admin: AdminHttpService) =
    new CassandraBackedStats(keyspace, period, TTL, collection)
}


/**
 * Save stats into cassandra database.
// * @param clusterName cassandra cluster name.
// * @param keyspaceName cassandra keyspace name.
// * @param seeds cassandra seeds.
 * @param period period.
 * @param TTL time to live.
 * @param collection collection.
 */
class CassandraBackedStats(keyspace:Keyspace,
                           val period:Duration,
                           val TTL:Int,
                           collection:StatsCollection)
  extends PeriodicBackgroundProcess("CassandraBackedCollector", period) {

  import scala.collection.JavaConversions._

  protected val logger = Logger.get()

  private lazy val COLUMN_FAMILY = CassandraBackedStatsConstant.COLUMN_FAMILY

  val listener = new StatsListener(collection)

  private val df = new SimpleDateFormat("yyyyMMdd")
  private var lastCollection: Time = Time.now

  private def dateNowF = df.format(new Date())


  def postfix(key:String) = "-" + dateNowF

  /**
   * Implement the periodic event here.
   */
  def periodic() {

    val stats = listener.get()

    for( (k , v) <- stats.counters ){
      insert("counter:" + k + postfix(k), v)
    }

    for( (k , v) <- stats.gauges ){
      insert("gauge:" + k + postfix(k), v)
    }

    stats.metrics.flatMap { case (key, distribution) =>
      distribution.toMap.map { case (subKey, value) =>
        insert("metric:" + key + "_" + subKey + postfix(key), value)
      }
    }

    lastCollection = Time.now

  }

  def insert(key:String, value:Double){
    insert(key, UUIDGen.getTimeUUID, value)
  }

  def insert(key:String, colName:UUID, colValue:Double){
//    logger.info("inserting key: %s, colName: %s, colValue: %s".format(key, colName, colValue))

    val ttl:java.lang.Integer = if (TTL > 0)
      TTL
    else
      null

    keyspace.prepareColumnMutation[String, UUID](COLUMN_FAMILY, key, colName)
      .withRetryPolicy(new BoundedExponentialBackoff(250, 5000, 10))
      .setConsistencyLevel(ConsistencyLevel.CL_ANY)
      .putValue(colValue, ttl)
      .executeAsync()
  }

  private def getLastHourInternal(key:String, limit:Int):List[List[Long]] = {
    val date = new Date()
    val start = UUIDGen.minTimeUUID(DateUtils.addHours(date, 1).getTime)
    val end = UUIDGen.maxTimeUUID(DateUtils.addHours(date, -1).getTime)

    getRange(key, start, end, limit)
  }

  /**
   * Get data by range.
   * @param key key name.
   * @param start started Date.
   * @param end ends Date.
   * @param limit max limit.
   * @return
   */
  def getRange(key:String, start:Date, end:Date, limit:Int): List[List[Long]] = {

    val startUUID = UUIDGen.minTimeUUID(start.getTime)
    val endUUID = UUIDGen.maxTimeUUID(end.getTime)

    getRange(key, startUUID, endUUID, limit)

  }

  /**
   * Get data by range.
   * @param key key name.
   * @param start started UUID, ex:
   *              `val start = UUIDGen.minTimeUUID(DateUtils.addHours(date, 1).getTime)`
   * @param end ends UUID, ex:
   *            val end = UUIDGen.maxTimeUUID(DateUtils.addHours(date, -1).getTime)
   * @param limit max limit.
   * @return
   */
  def getRange(key:String, start:UUID, end:UUID, limit:Int): List[List[Long]] = {

    val pKey = key + postfix(key)

    val cols = keyspace.prepareQuery(COLUMN_FAMILY)
      .getKey(pKey)
      .withColumnRange(start, end, true, limit)
      .execute().getResult

    val timings: List[List[Long]] = cols.map(x => List(uuidTimestampToUtc(x.getName.timestamp()) / 1000,
      x.getDoubleValue.toLong)).toList

    timings
  }


  /**
   * Get latest metrics data in last 60 minutes limited by :limit.
   * @param kind metrics kind, can be one of: counter, gauge, metric.
   * @param key metric key.
   * @param limit limit.
   * @return
   */
  def getLastHour(kind:String, key:String, limit:Int):List[List[Long]] = {

    import scala.collection.mutable

    val formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val cal = Calendar.getInstance()
    cal.set(Calendar.SECOND, 1)
    val todayWithZeroTime = formatter.parse(formatter.format(cal.getTime))


    val times = (for (i <- 0 until 60) yield ((todayWithZeroTime.getTime / 1000) - (i*60))).toList
    val timings = getLastHourInternal(kind + ":" + key, limit)

    var rv = mutable.Map(times.map(x => (x, 0L)).toSeq: _*)
    for ( t <- timings ){
      if (rv.contains(t(0))){
        rv += t(0) -> (rv.getOrElse(t(0),0L) + t(1))
      }else{
        rv += t(0) -> t(1)
      }
    }

    val z = rv.map(x => List(x._1, x._2)).toList.sortBy(_(0)).reverse

    z.slice(z.length - 60, (z.length - 60) + 60)

  }

  /**
   * Get latest metrics data for p50, minimum, average, and maximum, limited by :limit.
   * @param key metric key.
   * @param limit limit.
   * @return
   */
  def getMetricsLastHour(key:String, limit:Int):List[List[List[Long]]] = {

    import scala.collection.mutable

    val formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    cal.set(Calendar.SECOND, 1)
    val todayFirstSecond = formatter.parse(formatter.format(cal.getTime))


    val times = (for (i <- 0 until 60) yield ((todayFirstSecond.getTime / 1000) - (i*60))).toList

    val timings =
      getLastHourInternal("metric:" + key + "_msec_p50", limit) ::
      getLastHourInternal("metric:" + key + "_msec_minimum", limit) ::
      getLastHourInternal("metric:" + key + "_msec_average", limit) ::
      getLastHourInternal("metric:" + key + "_msec_maximum", limit) :: Nil

    var rv = List.empty[List[List[Long]]]
    for ( t <- timings){
      var m = mutable.Map(times.map(x => (x, 0L)).toSeq: _*)
      for ( z <- t ){
        if (m.contains(z(0))){
          m += z(0) -> (m.getOrElse(z(0), 0L) + z(1))
        }else{
          m += z(0) -> z(1)
        }
      }
      val z = m.map(x => List(x._1, x._2)).toList.sortBy(_(0)).reverse

      rv :+= z.slice(z.length - 60, (z.length - 60) + 60)
    }

    rv
  }
//
//  /**
//   * Get latest metrics data for p50, minimum, average, and maximum, limited by :limit.
//   * @param key
//   * @param start
//   * @param end
//   * @param limit
//   * @return
//   */
//  def getMetricsRange(key:String, start:Date, end:Date, unitMilis:Long, limit:Int):List[List[List[Long]]] = {
//
//    import scala.collection.mutable
//
//    val formatter = new SimpleDateFormat("dd/MM/yyyy")
//    val cal = Calendar.getInstance()
//    cal.setTime(start)
////    cal.set(Calendar.SECOND, 1)
////    val todayFirstSecond = formatter.parse(formatter.format(cal.getTime))
//
//
////    val times = (for (i <- 0 until 60) yield ((todayFirstSecond.getTime / 1000) - (i*60))).toList
//    val times = (for (i <- 0 until limit) yield ( (start.getTime / unitMilis) ))
//
//    val timings =
//      getRange("metric:" + key + "_msec_minimum-" + df.format(start), start, end, limit) ::
//      getRange("metric:" + key + "_msec_average-" + df.format(start), start, end, limit) ::
//      getRange("metric:" + key + "_msec_maximum-" + df.format(start), start, end, limit) :: Nil
////      getLastHourInternal("metric:" + key + "_msec_p50" + postfix(key), date, limit) ::
////      getLastHourInternal("metric:" + key + "_msec_minimum" + postfix(key), date, limit) ::
////      getLastHourInternal("metric:" + key + "_msec_average" + postfix(key), date, limit) ::
////      getLastHourInternal("metric:" + key + "_msec_maximum" + postfix(key), date, limit) :: Nil
//
//    var rv = List.empty[List[List[Long]]]
//    for ( t <- timings){
//      var m = mutable.Map(times.map(x => (x, 0L)).toSeq: _*)
//      for ( z <- t ){
//        if (m.contains(z(0))){
//          m += z(0) -> (m.getOrElse(z(0), 0L) + z(1))
//        }else{
//          m += z(0) -> z(1)
//        }
//      }
//      val z = m.map(x => List(x._1, x._2)).toList.sortBy(_(0)).reverse
//
//      rv :+= z.slice(z.length - 60, (z.length - 60) + 60)
//    }
//
//    rv
//  }

  /**
   * Convert UUID timestamp to epoch time UTC.
   * @param ts UUID timestamp.
   * @return
   */
  def uuidTimestampToUtc(ts:Long):Long = {
    val uuidEpoch = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    uuidEpoch.clear()
    uuidEpoch.set(1582, 9, 15, 0, 0, 0); // 9 = October
    val epochMillis = uuidEpoch.getTime().getTime()
    (ts / 10000L) + epochMillis
  }


}
