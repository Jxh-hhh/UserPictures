package UserFeaturesModel

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PeopleGender {
    def main(args: Array[String]): Unit = {
      def catalog =
        s"""{
           |"table":{"namespace":"default", "name":"tbl_users"},
           |"rowkey":"id",
           |"columns":{
           |"id":{"cf":"rowkey", "col":"id", "type":"string"},
           |"people":{"cf":"cf", "col":"people", "type":"string"}
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

      //汉族0、蒙古族1、回族2、藏族3、维吾尔族4、苗族5、满族6
      val result = readDF.select('id,
        when('people === "0", "汉族")
          .when('people === "1", "蒙古族")
          .when('people === "2", "回族")
          .when('people === "3", "藏族")
          .when('people === "4", "维吾尔族")
          .when('people === "5", "苗族")
          .when(condition = 'people === "6",  value = "满族")
          .otherwise("其他")
          .as("people")
      )

      def catalogWrite =
        s"""{
           |"table":{"namespace":"default", "name":"user_profile"},
           |"rowkey":"id",
           |"columns":{
           |"id":{"cf":"rowkey", "col":"id", "type":"string"},
           |"people":{"cf":"cf", "col":"people", "type":"string"}
           |}
           |}""".stripMargin

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }

}
