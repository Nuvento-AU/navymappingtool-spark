package au.com.nuvento.models

case class BusinessRulesQuery(
															 business_rule_id: Long,
															 business_rule_code: String,
															 business_rule_query: String,
															 business_rule_result: String,
															 business_rule_activeyn: Boolean,
															 business_rule_status: String,
															 business_rule_public_yn: Boolean,
															 business_rule_exclusive_yn: Boolean,
															 business_rule_download_id: String
														 )
