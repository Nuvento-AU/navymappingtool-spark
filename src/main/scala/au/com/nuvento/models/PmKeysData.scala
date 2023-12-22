package au.com.nuvento.models

case class PmKeysData(
											 row_id: Long,
											 business_rule_id: String,
											 business_rule_code: String,
											 json_row: String,
											 upload_id: String,
											 data_timestamp: String,
											 output: String,
											 processed_yn: Boolean
										 )
