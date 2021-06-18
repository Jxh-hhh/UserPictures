package UserFeaturesModel

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object XingZuoModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"XingZuo":{"cf":"cf", "col":"XingZuo", "type":"string"}
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


    val result = readDF.select('id,
      when('job === "1", "水瓶座")
        .when('job === "2", "双鱼座")
        .when('job === "3", "白羊座")
        .when('job === "4", "金牛座")
        .when('job === "5", "双子座")
        .when('job === "6", "巨蟹座")
        .when('job === "7", "狮子座")
        .when('job === "8", "处女座")
        .when('job === "9", "天秤座")
        .when('job === "10", "天蝎座")
        .when('job === "11", "射手座")
        .when('job === "12", "魔羯座")
        .otherwise("其他")
        .as("XingZuo")
    )

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"XingZuo":{"cf":"cf", "col":"Xingzuo", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}