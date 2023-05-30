package com.nasit

import org.apache.spark.sql.SparkSession

object ColumnLineageExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ColumnLineageExample")
      .master("local[*]")
      .getOrCreate()

    val orderDetailDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/order_details.csv")
    orderDetailDf.cache()
    orderDetailDf.createOrReplaceTempView("order_detail")

    val userDetailDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/user_info.csv")
    userDetailDf.cache()
    userDetailDf.createOrReplaceTempView("user_info")

    spark.listenerManager.register(new QueryListener)

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
    spark.stop()
  }
}
