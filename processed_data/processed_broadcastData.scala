// Databricks notebook source
//connect to the S3 bucket
val AccessKey = "XXXXXXXXXXXXXXXXXXXXXX"
val SecretKey = "XXXXXXXXXXXXXXXXXXXXXX"

val encoded_secret_key = SecretKey.replace("/", "%2F")
val AwsBucketName = "databrickprac"
val MountName = "mount_data34"

dbutils.fs.mount(s"s3a://$AccessKey:$encoded_secret_key@$AwsBucketName", s"/mnt/$MountName")

//display(dbutils.fs.ls(s"/mnt/$MountName"))

// COMMAND ----------

//list out all S3 folders
display(dbutils.fs.ls("/mnt/mount_data34"))

// COMMAND ----------

//list all mount names
%fs mounts

// COMMAND ----------

//Read the broadcast data from S3 bucket
val broadcastTrans = spark.read
  .option("infershcema","true")
  .option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/processed_data/processed_broadcast_Data/part-00000-tid-4245878298057660211-f00dd96a-1931-476f-aff2-495081883f08-44-1-c000.csv")

broadcast_Data.show(3)

//Read the stream data from S3 bucket
val streamData = spark.read
  .option("infershcema","true")
  .option("header","true")
  .csv("dbfs:/mnt/mount_data34/vardhan/processed_data/processed_stream_Data/part-00000-tid-3694408729991075661-2ae19345-2cff-463b-b894-fb0566d01c15-43-1-c000.csv")

stream_Data.show(3)

// s3://databrickprac/vardhan/processed_data/processed_broadcast_Data/part-00000-tid-4245878298057660211-f00dd96a-1931-476f-aff2-495081883f08-44-1-c000.csv

// COMMAND ----------

//modify the given file according to requirment 

import org.apache.spark.sql.functions._ 

 val transform_data = broadcastTrans.join(streamData, broadcastTrans
    .col("house_number") === streamData.col("house_number") &&
    broadcastTrans.col("country_code") === streamData.col("country_code"))
    .drop(broadcastTrans.col("house_number"))
    .drop(broadcastTrans.col("country_code"))
    .drop(broadcastTrans.col("dt"))
    .select("dt",
      "time",
      "device_name",
      "house_number",
      "user_id",
      "country_code",
      "program_title",
      "season",
      "season_episode",
      "genre",
      "product_type",
      "broadcast_right_start_date",
      "broadcast_right_end_date")
    .where(streamData("product_type") === "tvod" || streamData("product_type") === "est")
    .withColumn("load_date", current_date())
    .withColumn("year", year(col("load_date")))
    .withColumn("month", month(col("load_date")))
    .withColumn("day", dayofmonth(col("load_date")))

transform_data.show(3)

// COMMAND ----------

//writw data local to S3 bucket using spacific path 
transform_data.coalesce(1).write.partitionBy("year", "month", "day").option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/structured_data/structured_broadcast_Data")
