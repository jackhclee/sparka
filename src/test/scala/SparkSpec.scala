import com.bedrockstreaming.data.sparktest.{DataFrameEquality, SparkTestSupport}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import io.github.embeddedkafka.EmbeddedKafkaConfig.defaultConfig.kafkaPort
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serializer, StringDeserializer, StringSerializer}
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader.Deserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{explode, expr}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization

import java.time.Instant

case class Book(id: Int, name: String)

class SparkSpec extends AnyFlatSpec with Matchers with EmbeddedKafka with SparkTestSupport with DataFrameEquality {

  //  override lazy val additionalSparkConfiguration: Map[String, String] =
  //    Map("spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
  //      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  "main" should "do stuff" in { // A SparkSession `spark` is built in trait `SparkTestSupport`
    import spark.implicits._
    import scala.jdk.CollectionConverters._
    val ds1 = List("A", "B").toDF()
    val ds2 = List("A", "B").toDF()
    ds1 shouldEqual ds2
    val kafkaPort = 12345
    implicit val config = EmbeddedKafkaConfig(kafkaPort = kafkaPort)

    withRunningKafka {
      implicit val serializer   = new StringSerializer()
      implicit val deserializer = new StringDeserializer()
      val key   = "key"
      val value = "value"
      val topic = "topic"

      def createRec(ctr: Int): ProducerRecord[String, String] = {
        implicit val fmts : Formats = Serialization.formats(NoTypeHints)
        val rec = new ProducerRecord[String, String](
          topic,
          0,
          Instant.parse(s"2011-12-0${ctr}T10:15:30Z").toEpochMilli,
          s"K${ctr}",
          write(Book(ctr, "Apple"))
        )
        rec.headers().add("version", "10".getBytes)
        rec.headers().add("author", "jack".getBytes)
        rec
      }
      withProducer[String, String, Unit](producer => {
        Range(1,9).map( i => createRec(i)).foreach(rec => producer.send(rec))
      })
//      publishToKafka(topic = topic, message = "{\"id\":1,\"name\":\"Apple\"}")
//      publishToKafka(topic = topic, message = "{\"id\":2,\"name\":\"Banana\"}")

      val kafkaDf = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", s"127.0.0.1:$kafkaPort")
        .option("subscribe", topic)
        .option("includeHeaders", "true")
        .load
        .withColumn("key_str", $"key".cast("STRING"))
        .withColumn("value_str", $"value".cast("STRING"))
        .withColumn("exploded_header", explode($"headers"))
        .filter("exploded_header.key = 'version'")
        .withColumn("version", expr("exploded_header.value").cast("STRING"))

      kafkaDf.printSchema()
      println("1 ====")
      kafkaDf.show
      println("2 ====")

      val ds1 = kafkaDf.select($"value_str").as[String]
      ds1.printSchema()
      ds1.show(truncate = false)
      println("ds1!")
      println(s"ds1! ${ds1.count}")
      val jdss = spark.read.json(ds1)
      jdss.printSchema()
      jdss.show
      println("****!")

      val rds = List("{\"id\":1,\"name\":\"Apple\"}").toDS
      rds.show()
      println("rds!")
      val rjds = spark.read.json(rds)
      rjds.show
      println("rjds!")
    }
  }
}
