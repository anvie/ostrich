package com.twitter.ostrich.stats

import com.twitter.ostrich.admin.{PeriodicBackgroundProcess, AdminHttpService, StatsReporterFactory}
import com.twitter.util.{Time, Duration}
import com.netflix.astyanax.AstyanaxContext
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

/**
 * Author: robin
 * Date: 7/24/13
 * Time: 11:10 AM
 *
 */


/**
 * Create cassandra backed stats collection.
 * @param clusterName
 * @param keyspaceName
 * @param seeds
 * @param TTL
 * @param period
 */
class CassandraBackedStatsFactory(val clusterName:String, val keyspaceName:String,
                                  val seeds:String, val TTL:Int,
                                  val period:Duration) extends StatsReporterFactory {
  def apply(collection: StatsCollection, admin: AdminHttpService) =
    new CassandraBackedStats(clusterName,
      keyspaceName, seeds, period, TTL, collection)
}


/**
 * Save stats into cassandra database.
 * @param clusterName cassandra cluster name.
 * @param keyspaceName cassandra keyspace name.
 * @param seeds cassandra seeds.
 * @param period period.
 * @param TTL time to live.
 * @param collection collection.
 */
class CassandraBackedStats(val clusterName:String, val keyspaceName:String,
                           val seeds:String,
                           val period:Duration,
                           val TTL:Int,
                           collection:StatsCollection)
  extends PeriodicBackgroundProcess("CassandraBackedCollector", period) {

  import com.twitter.conversions.time._
  import scala.collection.JavaConversions._

  protected val logger = Logger.get()


  private val cb =
    new AstyanaxContext.Builder()
      .forCluster(clusterName)
      .forKeyspace(keyspaceName)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setCqlVersion("3.0.0")
        .setTargetCassandraVersion("1.2")
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
      )
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("ostrich-conn-poll")
        .setPort(9160)
        .setMaxConnsPerHost(20)
        .setInitConnsPerHost(10)
        .setSocketTimeout(30000)
        .setMaxTimeoutWhenExhausted(2000)
        .setSeeds(seeds)
      )
      .withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl)
  private val ctx = cb.buildKeyspace(ThriftFamilyFactory.getInstance())
  private val keyspace = {
    ctx.start()
    ctx.getClient
  }
  private val cluster = {
    val cc = cb.buildCluster(ThriftFamilyFactory.getInstance())
    cc.start()
    cc
  }

  private lazy val COLUMN_FAMILY = CassandraBackedStatsConstant.COLUMN_FAMILY

  val listener = new StatsListener(collection)

  private val df = new SimpleDateFormat("yyyyMMdd")

  private val PERCENTILES = List(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999)
  private val EMPTY_TIMINGS = List.fill(PERCENTILES.size)(0L)
  private var lastCollection: Time = Time.now

  private def dateNowF = df.format(new Date())

  /**
   * Implement the periodic event here.
   */
  def periodic() {

    val stats = listener.get()

    for( (k , v) <- stats.counters ){
      insert("counter:" + k + "-" + dateNowF, v)
    }

    for( (k , v) <- stats.gauges ){
      insert("gauge:" + k + "-" + dateNowF, v)
    }

    stats.metrics.flatMap { case (key, distribution) =>
      distribution.toMap.map { case (subKey, value) =>
        insert("metric:" + key + "_" + subKey + "-" + dateNowF, value)
      }
    }

    lastCollection = Time.now

  }

  def insert(key:String, value:Double){
    insert(key, UUIDGen.getTimeUUID, value)
  }

  def insert(key:String, colName:UUID, colValue:Double){
    logger.info("inserting key: %s, colName: %s, colValue: %s".format(key, colName, colValue))
    keyspace.prepareColumnMutation[String, UUID](COLUMN_FAMILY, key, colName)
      .putValue(colValue, TTL)
      .execute()
  }

  /**
   * Get latest metrics data limited by :limit.
   * @param kind metrics kind, can be one of: counter, gauge, metric.
   * @param key metric key.
   * @param date date range.
   * @param limit limit.
   * @return
   */
  def get(kind:String, key:String, date:Date, limit:Int):List[List[AnyVal]] = {

    val times = (for (i <- 0 until 60) yield (lastCollection + (i - 59).minutes).inSeconds).toList

    val start = UUIDGen.minTimeUUID(DateUtils.addDays(date, -1).getTime)
    val end = UUIDGen.maxTimeUUID(DateUtils.addDays(date, 1).getTime)
    val cols = keyspace.prepareQuery(COLUMN_FAMILY)
      .getKey(kind + ":" + key + "-" + df.format(date))
      .withColumnRange(end, start, true, limit)
      .execute().getResult

    var timings = cols.map(x => List(uuidTimestampToUtc(x.getName.timestamp()), x.getDoubleValue)).toList
    timings =
    if (timings.length < 60)
      List.fill(60 - timings.length)(List(0,0.0)) ++ timings
    else
      timings.slice(0, 60-1)
    val data = times.zip(timings).map { case (a, b) => a :: b(1) :: Nil }

    data
  }

  def uuidTimestampToUtc(ts:Long):Long = {
    val uuidEpoch = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    uuidEpoch.clear()
    uuidEpoch.set(1582, 9, 15, 0, 0, 0); // 9 = October
    val epochMillis = uuidEpoch.getTime().getTime()
    (ts / 10000L) + epochMillis
  }


  private var keyspaceEnsured = false
  private def ensureKeyspaceExists(){

    if (keyspaceEnsured)
      return

    // ensure keyspace exists
    val ctx = cluster.getClient

    var ksDef = ctx.describeKeyspace(keyspaceName)

    if (ksDef == null){
      logger.warning("Keyspace " + keyspaceName + " didn't exists, creating first.")
      var hm = new HashMap[String, String]()
      hm += "replication_factor" -> "1"
      ksDef = ctx.makeKeyspaceDefinition()
        .setName(keyspaceName)
        .setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
        .setStrategyOptions(hm)
      ctx.addKeyspace(ksDef)
      logger.info("keyspace created: " + keyspaceName)
    }

    keyspaceEnsured = true
  }

  private def ensureColumnFamilyExists(name:String){
    val ctx = cluster.getClient

    val ksDef = ctx.describeKeyspace(keyspaceName)

    var found = false
    if (ksDef != null){
      for (cdef <- ksDef.getColumnFamilyList){
        found |= cdef.getName.equals(name)
      }
    }

    if (!found){
      val cfDef = ctx.makeColumnFamilyDefinition()
        .setName(name)
        .setKeyspace(keyspaceName)
        .setComparatorType("org.apache.cassandra.db.marshal.TimeUUIDType")
      ctx.addColumnFamily(cfDef)
    }

  }

  override def start() {
    super.start()
    ensureKeyspaceExists()
    ensureColumnFamilyExists(CassandraBackedStatsConstant.COLUMN_FAMILY_NAME)
  }

  override def stop() {
    logger.info("Stopping " + getClass.getSimpleName + "...")
    super.stop()
    ctx.shutdown()
    cluster.shutdown()
  }
}

