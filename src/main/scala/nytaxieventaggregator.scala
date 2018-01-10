import java.sql.Timestamp
import java.text.SimpleDateFormat
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, SparkSession}

case class TaxiTripEvent(rateCodeID: Int, year: Int, tpep_pickup_datetime: Timestamp,
                         tpep_dropoff_datetime: Timestamp, passenger_count: Int,
                         trip_distance: Float, payment_type: Int, total_amount: Float) extends Serializable


object NyTaxiEventAggregator extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local[1]")
    .appName("NYTaxiTrips")
    .config("spark.cassandra.connection.host", "10.0.2.15")
    .getOrCreate()

  import spark.implicits._

  val cassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)

  cassandraConnector.withSessionDo(Session => Session.execute(
    "CREATE TABLE if not exists simplex.trips_byratecode (" +
      "rateCodeID INT," +
      "year INT," +
      "tpep_pickup_datetime TIMESTAMP, " +
      "tpep_dropoff_datetime TIMESTAMP," +
      "passenger_count INT," +
      "trip_distance FLOAT," +
      "payment_type INT," +
      "total_amount FLOAT," +
      "PRIMARY KEY ((rateCodeID, year), trip_distance)" +
      ") WITH CLUSTERING ORDER BY (trip_distance DESC);"))


  def saveEventCassandra(event: TaxiTripEvent) = {
    cassandraConnector.withSessionDo(Session=> {
      Session.execute(
        "INSERT INTO simplex.trips_byratecode (rateCodeID, year, tpep_pickup_datetime, tpep_dropoff_datetime," +
          "passenger_count, trip_distance, payment_type, total_amount) " +
          "VALUES (" +
          event.rateCodeID + "," +
          event.year + "," +
          event.tpep_pickup_datetime.getTime + "," +
          event.tpep_dropoff_datetime.getTime + "," +
          event.passenger_count + "," +
          event.trip_distance + "," +
          event.payment_type + "," +
          event.total_amount + ");")
    })
  }

  val kafkamsg = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "lineafichero")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]

  def createTaxiTrip(tripAtts: Array[String]) = {
    val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
    val lpep_pickup_datetime = new Timestamp(format.parse(tripAtts(1)).getTime)
    val lpep_dropoff_datetime = new Timestamp(format.parse(tripAtts(2)).getTime)
    val year = tripAtts(1).takeWhile(c => c != '-')

    TaxiTripEvent(tripAtts(4).trim.toInt, year.trim.toInt, lpep_pickup_datetime, lpep_dropoff_datetime,
      tripAtts(7).trim.toInt, tripAtts(8).trim.toFloat,
      tripAtts(17).trim.toInt, tripAtts(16).trim.toFloat)
  }

  val dfTaxiTrips = kafkamsg.map(_.split(',')).map(trip => createTaxiTrip(trip)).toDF()

  val parquetQuery = dfTaxiTrips.writeStream
    .format("parquet")
    .option("checkpointLocation", "hdfs://10.0.2.15/tmp/parquet")
    .option("path","hdfs://10.0.2.15/taxitrips")
    .start()

  val dswindow = dfTaxiTrips.select($"RateCodeID", $"year", $"tpep_pickup_datetime", $"tpep_dropoff_datetime"
    , $"Passenger_count", $"Trip_distance", $"Payment_type", $"Total_amount").as[TaxiTripEvent]

  val writerCassandra = new ForeachWriter[TaxiTripEvent] {

    override def open(partitionId: Long, version: Long) = true

    override def process(value: TaxiTripEvent) = {
      saveEventCassandra(value)
    }
    override def close(errorOrNull: Throwable) = {}
  }

  val queryCassandra = dswindow.writeStream.foreach(writerCassandra).start()

  parquetQuery.awaitTermination()

  queryCassandra.awaitTermination()
}
