import com.bedrockstreaming.data.sparktest.{DataFrameEquality, SparkTestSupport}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import io.github.embeddedkafka.EmbeddedKafkaConfig.defaultConfig.kafkaPort
import org.apache.kafka.common.serialization.{Serializer, StringDeserializer, StringSerializer}
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader.Deserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.expr
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      publishToKafka(topic = topic, message = "{\"id\":1,\"name\":\"Apple\"}")
      publishToKafka(topic = topic, message = "{\"id\":2,\"name\":\"Banana\"}")

      val kafkaDf = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", s"127.0.0.1:$kafkaPort")
        .option("subscribe", topic)
        .load
        .withColumn("valueStr", $"value".cast("STRING"))

      val ds1 = kafkaDf.select($"valueStr").as[String]
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
