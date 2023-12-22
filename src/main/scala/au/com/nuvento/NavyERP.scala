package au.com.nuvento

import au.com.nuvento.PmKeysMapping.mapExcelFile
import au.com.nuvento.models._
import au.com.nuvento.postgres.JdbcConnection
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NavyERP extends App {
	val jdbc = new JdbcConnection()
	val settings = new Settings().azure
	val accountName = settings.getString("storage-account-name")
	val accountKey = settings.getString("storage-account-key")
	val containerName = settings.getString("container-name")
	val spark = SparkSession.builder()
		.master("local[*]")
		.config("spark.files.overwrite", "true")
		.config(s"spark.hadoop.fs.azure.account.key.$accountName.blob.core.windows.net", accountKey)
		.appName("NavyERP").getOrCreate()
	Logger.getRootLogger.setLevel(Level.WARN)

	val upload_id = args(0)
		//"1c3cd0c1-fa1a-4e7f-b036-3fc75dc9073e"

	mapExcelFile(spark, upload_id, jdbc)
}
