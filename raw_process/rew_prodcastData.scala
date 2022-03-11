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

//Read the data from S3 bucket
val readdf = spark.read
  .option("infershcema","true")
  .option("header","true")
  .csv("dbfs:/mnt/mount_data34/vardhan/raw/broadcast_right.csv")

readdf.show(3)

// s3://databrickprac/vardhan/raw/broadcast_right.csv

// COMMAND ----------

readdf.createOrReplaceTempView("dataTable")
spark.sql("select * from dataTable").show(3)

// COMMAND ----------

//modify the given file according to requirment 
import org.apache.spark.sql.functions._

val trans_data =  readdf.withColumn("broadcast_right_vod_type", 
         lower(col("broadcast_right_vod_type")))
         .withColumn("load_date",current_date())
         .withColumn("country_code",
         when (col("broadcast_right_region") === "Denmark", "dk")
         .when (col("broadcast_right_region") === "Baltics", "ba")
         .when (col("broadcast_right_region") === "Bulgaria", "bg")
         .when (col("broadcast_right_region") === "Estonia", "ee")
         .when (col("broadcast_right_region") === "Finland", "fl")
         .when (col("broadcast_right_region") === "Latvia", "lv")
         .when (col("broadcast_right_region") === "Lithuania", "lt")
         .when (col("broadcast_right_region") === "Nordic", "nd")
         .when (col("broadcast_right_region") === "Norway", "no")
         .when (col("broadcast_right_region") === "Russia", "ru")
         .when (col("broadcast_right_region") === "Serbia", "rs")
         .when (col("broadcast_right_region") === "Slovenia", "si")
         .when (col("broadcast_right_region") === "Sweden", "se")
         .when (col("broadcast_right_region") === "VNH Region Group","vnh")
         .when (col("broadcast_right_region") === "Viasat History Region Group","vh")
         .otherwise("nocode"))

trans_data.show(2)

// COMMAND ----------

//writw data local to S3 bucket using spacific path 
trans_data.coalesce(1).write.option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/processed_data/processed_broadcast_Data/")
