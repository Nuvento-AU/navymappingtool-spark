package au.com.nuvento.utils
import play.api.libs.json._
class JSONToQuery {


	def jsonToSql(json: JsValue): String = {
		json match {
			case JsObject(fields) if fields.size == 1 =>
				fields.head match {
					case ("&&", conditions) =>
						conditions.as[JsArray].value.map(jsonToSql).mkString(" AND ")
					case ("||", conditions) =>
						conditions.as[JsArray].value.map(jsonToSql).mkString(" OR ")
					case (field, JsObject(operators)) =>
						operators.collect {
							case ("Equals", JsBoolean(value)) if value => s"$field=$field"
							case ("Equals", JsBoolean(value)) if !value => s"$field!=$field"
							case (op, JsString(value)) => s"$field$op$value"
						}.mkString(" AND ")
					case _ => "A"
				}
			case _ => "B"
		}
	}

}
