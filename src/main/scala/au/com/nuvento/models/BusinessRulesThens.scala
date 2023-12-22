package au.com.nuvento.models

case class BusinessRulesThens(
															 business_rule_thens_id: Long,
															 business_rule_id: Long,
															 then_statement: String,
															 processing_order: Long
)
