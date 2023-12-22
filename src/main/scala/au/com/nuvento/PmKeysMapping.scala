package au.com.nuvento

import au.com.nuvento.ExcelMapping.createExcelDataFrame
import au.com.nuvento.models.{BusinessRules, PositionData}
import au.com.nuvento.postgres.JdbcConnection
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PmKeysMapping {

	case class BusinessQuery(
														business_rule_query: String,
														business_rule_code: String,
														business_rule_result: String
														//Array[String]
										)


	def mapExcelFile(spark: SparkSession, upload_id: String, jdbc: JdbcConnection): Unit = {
		import spark.implicits._

		val roleAllocationLimit = jdbc.read(spark, "configuration")
			.where($"configuration_type" === "GLOBAL_BUSINESS_RULE_ALLOCATION")
			.selectExpr("get_json_object(configuration_value, '$[0].value') as value")
			.first()
			.getString(0).toInt
		val pmKeys = createExcelDataFrame(upload_id, spark)
			.withColumn("row_id", monotonically_increasing_id())
		val businessRules = jdbc.read(spark, "business_rules").as[BusinessRules]

		val queries: Array[BusinessQuery] = businessRules
			.where($"business_rule_activeyn" === true)
			.select($"business_rule_query", $"business_rule_code", $"business_rule_result")
			.as[BusinessQuery].collect
		val rulesResult: Array[(String, String, Array[Long])] = queries.flatMap { x =>
			try {
				val resultDF = pmKeys.filter(s"""${x.business_rule_query}""")
				val resultArray = resultDF.select("row_id").as(Encoders.scalaLong).collect()
				Some(x.business_rule_code, x.business_rule_result, resultArray)
			} catch {
				case e: Exception =>
					println("Exception: " + e.getMessage)
					None // or some other indicator of failure
			}
		}

		val resultDS: Array[(Long, String, String)] = rulesResult.flatMap { case (x, y, zArray) =>
			zArray.map(z => (z, x, y))
		}

		val result = resultDS
			.toList
			.toDF("rowId", "business_rule_code", "business_rule_result")
			.groupBy($"rowId")
			.agg(
				collect_set("business_rule_code").alias("business_rule_code"),
				collect_set("business_rule_result").alias("business_rule_result"),
				countDistinct("business_rule_code").alias("nerp_roles_count")
			)
			.withColumn("matched_yn", lit(true))


		val mappingResult = pmKeys
			.join(result, result("rowId") === pmKeys("row_id"), "left")
			//.drop($"row_id")
			.drop($"rowId")


		val positionData: DataFrame = mappingResult.select(
			//$"row_id",
			//$"data_entry_timestamp",
			$"POSNNBR".alias("position_id").cast(IntegerType),
			$"POSNTITLE".alias("position_title"),
			$"DEPTID".alias("department_id").cast(IntegerType),
			$"DEPTDESC".alias("department_description"),
			$"UNITID".alias("unit_id").cast(IntegerType),
			$"UNITDESC".alias("unit_description"),
			$"RANKLEVEL".alias("rank_level"),
			$"LOC".alias("loc"),
			$"LOCALITY".alias("locality"),
			$"STATE".alias("state_loc"),
			$"business_rule_code".alias("matched_business_rule_number"),
			$"business_rule_result".alias("nerp_proposed_role"),

			$"nerp_roles_count".cast(IntegerType)
		)
			.withColumn("matched_business_rule_number", concat_ws(", ", $"matched_business_rule_number"))
			.withColumn("nerp_proposed_role", concat_ws(", ", $"nerp_proposed_role"))
			.withColumn("upload_id", lit(upload_id))
			.withColumn("data_entry_timestamp", now())
			.withColumn("derp_proposed_role", lit(""))
			.withColumn("exceeds_role_allocation_limit", when(col("nerp_roles_count") > roleAllocationLimit, true)
				.otherwise(false))
			.withColumn("bypass_role_allocation_limit", lit(false))

		val mapCount = mappedRoleCount(spark, upload_id, jdbc, businessRules)

		jdbc.write(spark, "mapped_roles_count", mapCount, SaveMode.Append)
		jdbc.write(spark, "position_data", positionData, SaveMode.Append)
	}

	def mappedRoleCount(spark: SparkSession, upload_id: String, jdbc: JdbcConnection, businessRules: Dataset[BusinessRules]): DataFrame = {
		import spark.implicits._
		val positionData = jdbc.read(spark, "position_data").as[PositionData]
		val psnDataById = positionData.filter($"upload_id" === upload_id)
		val psnArrayExplode = psnDataById.select($"matched_business_rule_number")
			.withColumn("matched_business_rule_number",
				explode(
					split(
						trim(regexp_replace($"matched_business_rule_number", "\\s", "")),
						",")
				)
			)
			.filter($"matched_business_rule_number".isNotNull && $"matched_business_rule_number" =!= "")
		val mapCount = psnArrayExplode.groupBy($"matched_business_rule_number").count()
		val mapCountJoin = mapCount.joinWith(businessRules, businessRules("business_rule_code") === mapCount("matched_business_rule_number"))
		val mapped_role_count = mapCountJoin.withColumn("upload_id", lit(upload_id))
			.select($"_2.business_rule_id", $"upload_id", col("_1.count").alias("mapped_count"))

		mapped_role_count
	}
}
