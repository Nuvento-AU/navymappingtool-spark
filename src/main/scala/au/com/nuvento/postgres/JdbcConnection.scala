package au.com.nuvento.postgres

import au.com.nuvento.Settings
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

class JdbcConnection {

	val settings = new Settings().postgres
	val properties = new Properties()
	properties.put("user", settings.getString("user"))
	properties.put("password", settings.getString("pwd"))
	properties.put("driver", "org.postgresql.Driver")
	properties.put("SaveMode", "Append")
	val jdbcURL: String = "jdbc:postgresql://" + settings.getString("host") + ":" + settings.getString("port") +  "/" + settings.getString("db")
	println(jdbcURL)

	def write(spark: SparkSession, table: String, data: DataFrame, saveMode: SaveMode): Unit = {
		try {
			data.write
				.mode(saveMode)
				.jdbc(jdbcURL, table, properties)
		} catch {
			case e: Exception => e.printStackTrace()
				throw e
		}
	}
	def read(spark: SparkSession, table: String): DataFrame = {
		try {
			spark.read.jdbc(jdbcURL, table, properties)
//		spark.read
//		.format("jdbc")
//		.option("url", jdbcURL)
//		.option("dbtable", table)
//		.option("user", settings.getString("user") )
//		.option("password", settings.getString("pwd"))
//		.option("driver", "org.postgresql.Driver")
//		.load()
		} catch {
			case e: Exception => e.printStackTrace()
				throw e
		}
	}
}
