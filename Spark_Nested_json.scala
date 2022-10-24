// Databricks notebook source
val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "test_json").option("collection", "nested").load()

// COMMAND ----------

df.show()

// COMMAND ----------

val ordersDf=df.select("datasets","filename")

// COMMAND ----------

import org.apache.spark.sql.functions._
var parseOrdersDf = ordersDf.withColumn("orders", explode($"datasets"))

// COMMAND ----------

parseOrdersDf.show()

// COMMAND ----------

parseOrdersDf = parseOrdersDf.withColumn("customerId", $"orders".getItem("customerId"))
                             .withColumn("orderId", $"orders".getItem("orderId"))
                             .withColumn("orderDate", $"orders".getItem("orderDate"))
                             .withColumn("orderDetails", $"orders".getItem("orderDetails"))
                             .withColumn("shipmentDetails", $"orders".getItem("shipmentDetails"))

// COMMAND ----------

parseOrdersDf.show()

// COMMAND ----------

 parseOrdersDf = parseOrdersDf.withColumn("orderDetails", explode($"orderDetails"))

// COMMAND ----------

parseOrdersDf.show()

// COMMAND ----------

parseOrdersDf = parseOrdersDf.withColumn("productId", $"orderDetails".getItem("productId"))
                             .withColumn("quantity", $"orderDetails".getItem("quantity"))
                             .withColumn("sequence", $"orderDetails".getItem("sequence"))
                             .withColumn("totalPrice", $"orderDetails".getItem("totalPrice"))
                             .withColumn("city", $"shipmentDetails".getItem("city"))
                             .withColumn("country", $"shipmentDetails".getItem("country"))
                             .withColumn("postalcode", $"shipmentDetails".getItem("postalCode"))
                             .withColumn("street", $"shipmentDetails".getItem("street"))
                             .withColumn("state", $"shipmentDetails".getItem("state"))

// COMMAND ----------

parseOrdersDf.show()

// COMMAND ----------

 parseOrdersDf = parseOrdersDf.withColumn("gross", $"totalprice".getItem("gross"))
                             .withColumn("net", $"totalprice".getItem("net"))
                             .withColumn("tax", $"totalprice".getItem("tax"))

// COMMAND ----------

parseOrdersDf.show()

// COMMAND ----------

val jsonParseOrdersDf = parseOrdersDf.select($"orderId"
                                           ,$"customerId"
                                           ,$"orderDate"
                                           ,$"productId"
                                           ,$"quantity"
                                           ,$"sequence"
                                           ,$"gross"
                                           ,$"net"
                                           ,$"tax"
                                           ,$"street"
                                           ,$"city"
                                           ,$"state"
                                           ,$"postalcode"
                                           ,$"country")

// COMMAND ----------

display(jsonParseOrdersDf)

// COMMAND ----------

jsonParseOrdersDf.show()

// COMMAND ----------

jsonParseOrdersDf.createOrReplaceTempView("price")

// COMMAND ----------

val Employee = spark.sql("SELECT t.customerId,t.orderId,t.net FROM (SELECT customerId,orderId, net,DENSE_RANK() OVER (partition by customerId ORDER BY net DESC) AS DENSE_RANK FROM price)as t WHERE t.dense_rank = 2")

// COMMAND ----------

display(Employee)
