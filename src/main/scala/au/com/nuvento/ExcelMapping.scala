package au.com.nuvento

import au.com.nuvento.NavyERP.{accountName, containerName}
import au.com.nuvento.utils.ColumnNameParser.mapColumns
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory
import org.apache.poi.ss.usermodel.{Cell, DataFormatter, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.io.ByteArrayInputStream



object ExcelMapping extends App{


//	case class ExcelBinary(
//													path: String,
//													modificationTime: Timestamp,
//													length: Long,
//													content: Binary
//												)

def createExcelDataFrame(upload_id: String, spark: SparkSession): DataFrame = {
	import scala.collection.JavaConverters._
	// Read the contents of the workbook
	import spark.implicits._
	//println(containerName + " : " + accountName)
	// Define Azure Blob Storage URL
	val blobStorageUrl = s"wasbs://$containerName@$accountName.blob.core.windows.net/$upload_id.xlsx"

	val excelBinary = spark.read.format("binaryFile").load(blobStorageUrl)
		//.as[ExcelBinary]
	// Extract file content
	val fileContent = excelBinary.select("content").first.getAs[Array[Byte]](0)

	// next two lines are for fixing this bug:
	// https://stackoverflow.com/questions/67884617/apache-poi-excel-writer-works-in-ide-but-not-in-fat-jar-java-io-ioexception-yo
	WorkbookFactory.addProvider(new HSSFWorkbookFactory)
	WorkbookFactory.addProvider(new XSSFWorkbookFactory)

	val workbook = WorkbookFactory.create(new ByteArrayInputStream(fileContent))
	//	val df = spark.read
//		.format("com.crealytics.spark.excel")
//		.option("header", "true")
//		//.option("usePlainNumberFormat", "true")
//		.option("inferSchema", "true")
//		.load(blobStorageUrl)
//	df
	//val result = mapColumns(df)

	//	result
//	val filePath = SparkFiles.getRootDirectory() + "/" + fileName
//	println("filePath: " + filePath)
//	val excelFile = new File(filePath)
//
//
	//val workbook = WorkbookFactory.create(excelFile)

		var data = List[List[String]]()

		val excelData = workbook.getSheetAt(0)
		val formatter = new DataFormatter()
		for (row <- excelData.asScala) {
			val rowData = row.iterator().asScala.map {
				case cell: Cell => formatter.formatCellValue(cell)
				case _ => ""
			}.toList
			if (rowData.size > 3) {
				data :+= rowData
			}
		}
		workbook.close()

		val schema = StructType(excelData.getRow(0).iterator().asScala
			.map(cell => StructField(cell.toString, StringType))
			.toList)
	//schema.printTreeString()
		val rdd = spark.sparkContext.parallelize(data.drop(1))
		val df = spark.createDataFrame(rdd.map(row => Row(row: _*)), schema)

		val result = mapColumns(df)
	//result.show()
	result
	}
}
