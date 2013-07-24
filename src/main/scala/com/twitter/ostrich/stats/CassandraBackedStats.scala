package com.twitter.ostrich.stats

import com.twitter.ostrich.admin.{PeriodicBackgroundProcess, AdminHttpService, StatsReporterFactory}
import com.twitter.util.Duration
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



class CassandraBackedStatsFactory(val clusterName:String, val keyspaceName:String,
                                  val seeds:String,
                                  val period:Duration) extends StatsReporterFactory {
  def apply(collection: StatsCollection, admin: AdminHttpService) =
    new CassandraBackedStats(clusterName,
      keyspaceName, seeds, period, collection)
}



class CassandraBackedStats(val clusterName:String, val keyspaceName:String,
                           val seeds:String,
                           val period:Duration,
                           collection:StatsCollection)
  extends PeriodicBackgroundProcess("CassandraBackedCollector", period) {

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

  private def dateNowF = df.format(new Date())

  /**
   * Implement the periodic event here.
   */
  def periodic() {

    val stats = listener.get()

    for( (k , v) <- stats.counters ){
      insert(k + "-" + dateNowF, v)
    }

  }

  def insert(key:String, value:Double){
    insert(key, UUIDGen.getTimeUUID, value)
  }

  def insert(key:String, colName:UUID, colValue:Double){
    logger.info("inserting key: %s, colName: %s, colValue: %s".format(key, colName, colValue))
    keyspace.prepareColumnMutation[String, UUID](COLUMN_FAMILY, key, colName)
      .putValue(colValue, 604800) // TTL for one week
      .execute()
  }

  def get(key:String, date:Date, limit:Int) = {

    /**
     * Testing get data
     */
    val start = UUIDGen.minTimeUUID(DateUtils.addDays(date, -1).getTime)
    val end = UUIDGen.maxTimeUUID(DateUtils.addDays(date, 1).getTime)
    val cols = keyspace.prepareQuery(COLUMN_FAMILY)
      .getKey(key + "-" + df.format(date))
      .withColumnRange(end, start, true, limit)
      .execute().getResult

    for (col <- cols)
      yield (col.getName.timestamp(), col.getDoubleValue)


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

