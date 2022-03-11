// Databricks notebook source
//connect to the S3 bucket
val AccessKey = "XXXXXXXXXXXXXXXXXXXXXX"
val SecretKey = "XXXXXXXXXXXXXXXXXXXXXX"

val encoded_secret_key = SecretKey.replace("/", "%2F")
val AwsBucketName = "databrickprac"
val MountName = "mount_data34"

dbutils.fs.mount(s"s3a://$AccessKey:$encoded_secret_key@$AwsBucketName", s"/mnt/$MountName")

// COMMAND ----------

//list out all S3 folders
display(dbutils.fs.ls("/mnt/mount_data34"))

// COMMAND ----------

//Read the data from S3 bucket
val readdf = spark.read
  .option("infershcema","true")
  .option("header","true")
  .option("sep",";")
  .csv("dbfs:/mnt/mount_data34/vardhan/raw/started_streams.csv")
readdf.show(3)

// COMMAND ----------

//modify the given file according to requirment 
import org.apache.spark.sql.functions._

val trans_data = readdf
   .withColumn("load_date",current_date())

trans_data.show(3)

// COMMAND ----------

// dbutils.fs.put(s"/mnt/$MountName", "s3://databrickprac/vardhan/processed_data/")

// trans_data
// .write 
// .format("com.databricks.spark.csv") 
// .option("header", "true") 
// .save("s3://{}:{}@{}/{}".format(AccessKey, SecretKey, AwsBucketName, "s3://databrickprac/vardhan/processed_data"))

 trans_data.coalesce(1).write.option("header","true").csv("dbfs:/mnt/mount_data34/vardhan/processed_data/processed_stream_Data/")



