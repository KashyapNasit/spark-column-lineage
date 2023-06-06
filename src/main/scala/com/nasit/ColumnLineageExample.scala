package com.nasit

import org.apache.spark.sql.SparkSession

object ColumnLineageExample {
  def main(args: Array[String]): Unit = {

    //Create Spark Session.
    val spark = SparkSession.builder()
      .appName("ColumnLineageExample")
      .master("local[*]")
      .getOrCreate()

    //Read order detail CSV file.
    val orderDetailDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/order_details.csv")

    //Put it in cache.
    orderDetailDf.cache()

    //Create a view from it.
    orderDetailDf.createOrReplaceTempView("order_detail")

    //Read from user_info CSV file.
    val userDetailDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/user_info.csv")

    //Put it in cache.
    userDetailDf.cache()

    //Create a view from it.
    userDetailDf.createOrReplaceTempView("user_info")

    //We are registering a listener. This can be done via `--conf "spark.extraListeners=com.package.ListenerClass"` as well while submitting spark application
    spark.listenerManager.register(new QueryListener)

    // A sample query which joins both the data sources and gives avg order value with full name of customer.
    val sqlQuery =
      """
        |SELECT
        |  f_name || " " || l_name as full_name,
        |  avg(amount) as avg_order_value
        |FROM
        |  (SELECT
        |     user_id as u_id,
        |     order_amount as amount
        |   FROM
        |     order_detail) o
        |  INNER JOIN
        |  (SELECT
        |     first_name as f_name,
        |     last_name as l_name,
        |     user_id as uid
        |   FROM
        |     user_info) u
        |      ON
        |      u.uid = o.u_id
        |GROUP BY
        |  1
        |ORDER BY
        |  2
        |""".stripMargin

    val queryResult = spark.sql(sqlQuery)
    queryResult.show(false)

    orderDetailDf.unpersist()
    userDetailDf.unpersist()

    spark.stop()
  }
}
