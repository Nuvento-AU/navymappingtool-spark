package au.com.nuvento.models

case class BusinessRulesWhens(
															 business_rule_whens_id: Long,
															 business_rule_id: Long,
															 oy: String,
															 vey: String,
															 comparison: String,
															 processing_order: Long
														 )
