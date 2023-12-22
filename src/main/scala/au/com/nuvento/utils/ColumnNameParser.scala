package au.com.nuvento.utils

import org.apache.spark.sql.DataFrame

object ColumnNameParser {

	//Rename a column name string into a standard CamelCase format e.g. "O'Seas Grouping" -> "OSeasGrouping"
	def renameField(fieldName: String): String = {
		fieldName.trim.toUpperCase
			.replaceAll("\\b(DESCR(?:IPTION)?)\\b", "DESC")
		.replaceAll("POSITION", "POSN")
		.replaceAll("%", "PERCENT")
		.replaceAll("#", "NBR")
		.replaceAll("\\$|'|-|\\/|\\W", "")
	}

	//for each of the columns in the json_row.pmKeys_data table use the renamefield() function to standardize the name
	def mapColumns(df: DataFrame): DataFrame = {
		var resultDF = df
		df.columns.foreach { col =>
			resultDF = resultDF.withColumnRenamed(col, renameField(col))
		}
		resultDF
	}

}
