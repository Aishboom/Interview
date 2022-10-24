// Databricks notebook source
val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "test").option("collection", "employee_card_punch_details_new").load()

// COMMAND ----------

df.show()

// COMMAND ----------

val df1 = df.select("Emp Code","Punch - IN","Punch - Out")

// COMMAND ----------

display(df1)

// COMMAND ----------

df1.write.format("csv").mode("overwrite").save("/FileStore/tables/data_preview.csv")
