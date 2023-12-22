package au.com.nuvento
//import au.com.nuvento.models.BusinessRules
import au.com.nuvento.NavyERP.jdbc
import au.com.nuvento.models._
import au.com.nuvento.postgres.JdbcConnection
import au.com.nuvento.utils.ColumnNameParser.renameField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions}

object BusinessRulesMapping {

	case class BusinessRuleList(
															 brId: Long,
															 brCode: String,
															 keyValue: String
														 )
	case class BusinessRuleGroup(
															 brId: Long,
															 brCode: String,
															 query: String,
															 brResult: Array[String]
														 )

	def renameBusinesRuleWhensColumns(brWhens: Dataset[BusinessRulesWhens], spark: SparkSession): Dataset[BusinessRulesWhens] = {
		import spark.implicits._
		brWhens.withColumn("oy",
			when(col("oy") === "LX2", "LX2GROUP")
				.when(col("oy") === "LX3", "LX3GROUP")
				.when(col("oy") === "LX4", "LX4DIVISION")
				.when(col("oy") === "LX5", "LX5BRANCH")
				.when(col("oy") === "LX6", "LX6DIRECTORATE")
				.when(col("oy") === "LX7", "LX7UNITSPO")
				.when(col("oy") === "LX8", "LX4")
				.otherwise(col("oy"))
		).as[BusinessRulesWhens]
	}

	def createBRQueryFromWhens(spark: SparkSession): DataFrame  = {
		import spark.implicits._
		val businessRules = jdbc.read(spark, "business_rules").as[BusinessRules]
		val businessRulesWhens = jdbc.read(spark, "business_rule_whens").as[BusinessRulesWhens]
		val brWhensRenamed = renameBusinesRuleWhensColumns(businessRulesWhens, spark)
		val businessRulesThens = jdbc.read(spark, "business_rule_thens").as[BusinessRulesThens]
		//val businessRulesUsers = jdbc.read(spark, "business_rules_user").as[BusinessRulesUser]
		val brWhenThen = brWhensRenamed.joinWith(businessRulesThens, brWhensRenamed("business_rule_id") === businessRulesThens("business_rule_id"))
		val brDatasetJoin = businessRules.joinWith(brWhenThen, businessRules("business_rule_id") === brWhenThen("_1.business_rule_id"))
		val brList = brDatasetJoin.map {
			x =>
				val key = renameField(x._2._1.oy)
				val keyValue = (s"""($key="${x._2._1.vey}")""")
				val brCode = x._1.business_rule_code
				val brId = x._1.business_rule_id

				BusinessRuleList(brId, brCode,keyValue)
		}.as[BusinessRuleList]
		val brGroup = brList.groupBy($"brId")
			.agg(
				collect_set($"brCode").alias("brCode"),
				concat_ws("AND", collect_list("keyValue")).alias("query"),
				collect_set($"brResult").alias("brResult")
			).withColumn("brCode", functions.expr("brCode[0]")).as[BusinessRuleGroup]
		val brQuery = brList.join(businessRules, businessRules("business_rule_id") === brGroup("brId"))
		//val brFinal = brQuery.join(businessRulesUsers, brQuery("brId") === businessRulesUsers("business_rule_id"))
		brQuery.select(
				col("brId").alias("business_rule_id"),
				col("brCode").alias("business_rule_code"),
				col("query").alias("business_rule_query"),
				col("business_rule_result").alias("business_rule_result"),
				col("business_rule_activeyn"),
				col("business_rule_status"),
				col("business_rule_public_yn"),
				col("exclusive_yn").alias("business_rule_exclusive_yn"),
				col("business_rule_json").alias("business_rule_json"),
			col("user_id"),
			col("created_date"),
			col("updated_date")
			)
			//.as[BusinessRulesQuery]
	}
	def createBusinessRulesQueryTable(spark: SparkSession, jdbc: JdbcConnection): Unit = {
		val table = createBRQueryFromWhens(spark).toDF()
		table.printSchema()
		table.show(false)
		jdbc.write(spark, "business_rules_query", table, SaveMode.Overwrite)
	}
}