package au.com.nuvento.models

import java.sql.Timestamp

case class BusinessRulesUser(
															business_rule_id: Long,
															user_id: Long,
															created_at: Timestamp
														)
