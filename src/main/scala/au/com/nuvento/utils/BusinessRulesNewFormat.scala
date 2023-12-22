package au.com.nuvento.utils

class BusinessRulesNewFormat {

//	case class BusinessRules(
//														business_rule_id: Long,
//														business_rule_code: String,
//														business_rule_description: String,
//														business_rule_activeyn: Boolean,
//														business_rule_status: String,
//														business_rule_public_yn: Boolean,
//														processing_order: Int,
//														exclusive_yn: Boolean,
//														download_id: String
//													)
//
//	case class BusinessRulesWithQuery(
//																		 business_rule_id: Long,
//																		 business_rule_code: String,
//																		 business_rule_description: String,
//																		 business_rule_activeyn: Boolean,
//																		 business_rule_status: String,
//																		 business_rule_public_yn: Boolean,
//																		 processing_order: Int,
//																		 exclusive_yn: Boolean,
//																		 download_id: String,
//																		 business_rule_query: String
//																	 )
//
//	def mapBusinessRuleQuery(br: DataFrame, spark: SparkSession): Dataset[BusinessRulesWithQuery] = {
//		import spark.implicits._
//		val brDataset = br.as[BusinessRules]
//			brDataset.map(x => BusinessRulesWithQuery(
//			x.business_rule_id,
//			x.business_rule_code,
//			x.business_rule_description,
//			x.business_rule_activeyn,
//			x.business_rule_status,
//			x.business_rule_public_yn,
//			x.processing_order,
//			x.exclusive_yn,
//			x.download_id,
//			formatDescription(x.business_rule_description)
//		))
//	}
//
//	def formatDescription(brDescription: String): String = {
//
//		brDescription.split(" ").map(x => x match {
//			case "&&" => ")AND("
//			case "when" => "("
//			case "==" => "="
//			case "then" => ") then"
//			case _ => x
//
//		}).mkString("").replaceAll("(.*)then.*", "$1")
//	}

}
