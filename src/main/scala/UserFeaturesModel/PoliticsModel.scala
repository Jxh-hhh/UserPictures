package UserFeaturesModel

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PoliticsModel {

    def main(args: Array[String]): Unit = {
      def catalog =
        s"""{
           |"table":{"namespace":"default", "name":"tbl_users"},
           |"rowkey":"id",
           |"columns":{
           |"id":{"cf":"rowkey", "col":"id", "type":"string"},
           |"politics":{"cf":"cf", "col":"politics", "type":"string"}
           |}
           |}""".stripMargin

      val spark = SparkSession.builder()
        .appName("shc test")
        .master("local[10]")
        .getOrCreate()

      import spark.implicits._

      val readDF: DataFrame = spark.read
        .option(HBaseTableCatalog.tableCatalog, catalog)
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

      //1群众、2党员、3无党派人士
      val result = readDF.select('id,
        when('politics === "1", "public")
          .when('politics === "2", "dangyuan")
          .when('politics === "3", "null")
          .otherwise("其他")
          .as("politics")
      )

      def catalogWrite =
        s"""{
           |"table":{"namespace":"default", "name":"user_profile"},
           |"rowkey":"id",
           |"columns":{
           |"id":{"cf":"rowkey", "col":"id", "type":"string"},
           |"politics":{"cf":"cf", "col":"politics", "type":"string"}
           |}
           |}""".stripMargin

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }
}
