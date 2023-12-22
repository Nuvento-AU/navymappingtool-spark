package au.com.nuvento.models

import java.sql.Timestamp

case class BusinessRules(
													business_rule_id: Long,
													business_rule_code: String,
													business_rule_description: String,
													business_rule_activeyn: Boolean,
													business_rule_status: String,
													business_rule_public_yn: Boolean,
													processing_order: Int,
													exclusive_yn: Boolean,
													business_rule_json: String,
													business_rule_query: String,
													business_rule_result: String,
													user_id: Long,
													created_date: Timestamp,
													updated_date: Timestamp
												)
