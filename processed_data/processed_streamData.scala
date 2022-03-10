// Databricks notebook source
//connect to the S3 bucket
val AccessKey = "AKIA22UAFDPYYDZBIXBL"
val SecretKey = "hklNXmXCRwyqgN/5FjBvxYUzchUrZ3egN39VT4Zs"

val encoded_secret_key = SecretKey.replace("/", "%2F")
val AwsBucketName = "databrickprac"
val MountName = "mount_data34"

dbutils.fs.mount(s"s3a://$AccessKey:$encoded_secret_key@$AwsBucketName", s"/mnt/$MountName")

// COMMAND ----------

//list out all S3 folders
display(dbutils.fs.ls("/mnt/mount_data34"))

// COMMAND ----------

//list all mount names
%fs mounts

// COMMAND ----------

//Read the data from S3 bucket
val readdf = spark.read.option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/processed_data/processed_stream_Data/part-00000-tid-3694408729991075661-2ae19345-2cff-463b-b894-fb0566d01c15-43-1-c000.csv")
readdf.show(3)



// COMMAND ----------

//modify the given file according to requirment 

import org.apache.spark.sql.functions._ 

val transform_data = readdf
        .select(col("dt"),col("device_name"),col("product_type"),
        col("user_id"),col("program_title"), col("country_code"))
        .groupBy(col("dt"), col("program_title"), col("device_name"),
        col("country_code"), col("product_type"))
        .agg(countDistinct(col("user_id")) as "unique_users", 
        count(col("program_title")) as "content_count")
        .withColumn("load_date", current_date())
        .withColumn("year", year(col("load_date")))
        .withColumn("month", month(col("load_date")))
        .withColumn("day", dayofmonth(col("load_date")))
        .sort(col("unique_users").desc) 

transform_data.show(3)

// COMMAND ----------

//writw data local to S3 bucket using spacific path 
transform_data.write.partitionBy("year", "month", "day").option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/structured_data/structured_stream_Data/")

// s3://databrickprac/vardhan/structured_data/