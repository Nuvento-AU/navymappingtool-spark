package au.com.nuvento.models

import java.sql.Timestamp

case class PositionData(
												 row_id: Long,
												 data_entry_timestamp: Timestamp,
												 position_id: Long,
												 position_title: Option[String],
												 department_id: Option[Long],
												 department_description: Option[String],
												 unit_id: Option[Long],
												 unit_description: Option[String],
												 rank_level: Option[String],
												 loc: Option[String],
												 locality: Option[String],
												 state_loc: Option[String],
												 matched_business_rule_number: Option[String],
												 nerp_proposed_role: Option[String],
												 upload_id: String,
												 derp_proposed_role: Option[String],
												 matched_yn: Option[Boolean],
												 exceeds_role_allocation_limit: Option[Boolean],
												 bypass_role_allocation_limit: Option[Boolean],
												 nerp_roles_count: Option[Long]
											 )
