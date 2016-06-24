import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import java.io._
import java.util.Date
import java.text.SimpleDateFormat

object FirstApp {
	
	val conf = new SparkConf().setAppName("AverageSpeed")
    val sc = new SparkContext(conf)

    val path = "/Users/cassiohg/Coding/Scala/riobus-report-average-speed/" // path to project.
	val filenameAndPath = "hdfs://localhost:8020/riobusData/estudo_cassio_part_00000000000[0-2]*" // path to file being read.
	val resultFilenameAndPath = path + "average-result.txt" // path to file that will be written.

	val dateFormatGoogle = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") // format used by the data we have.
	val dateFormathttp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss") // format we use inside http message.

	def main(args: Array[String]) {
		
		// value date that will hold the begining of the date interval.
		val dateBegin = dateFormathttp.parse(args(0)) // value date that will hold the beggining of the date interval.
		val dateEnd = dateFormathttp.parse(args(1)) // value date that will hold the end of the date interval.

		// rectangle specified by an anchor point, a length and a height. the anchor point is in the top left corner.
		val latitude1 = args(2).toDouble // value date that will hold the rectangle's bottom left latitude.
		val longitude1 = args(3).toDouble // value date that will hold the rectangle's bottom left longitude.
		val latitude2 = args(4).toDouble // value date that will hold rectangle's top right latitude.
		val longitude2 = args(5).toDouble // value date that will hold rectangle's top right longitude.

		// returns true if 'stringDate', converted to Date object is bigger than 'dateBeggin' and smaller than 'dateEnd'.
		val isdateInsideInterval = {(stringDate: String) =>
			//converting string to a date using pattern inside 'dateFormatGoogle'.
			var date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(stringDate)
			// appearenlty I can't use the 'dateFormatGoogle' because 'SimpleDateFormat' is not thread safe. I need a
			// new instance for every thread, but I don't know how to do it in an spark application. That's why I create a
			// new instance inside every function call.

			// testing if date is inside date interval.
			date.compareTo(dateBegin) >= 0 && date.compareTo(dateEnd) <= 0
		}

		// returns true if latitude and longitude are inside the rectangle defined by the 
		// arguments given to this application.
		val isInsideRectangle = {(stringX: String, stringY: String) =>
			var lat = stringX.toDouble // converting string to double.
			var lng = stringY.toDouble // converting string to double.
			// testing if lat and longi are inside ractangle boundaries.
			lat >= latitude1 && lng >= longitude1 && lat <= latitude2 && lng <= longitude2
		}

		/* reading file from hadoop hdfs that is listening on localhost:8020. you should put the file inside it before 
		running this code. 
		$ hadoop fs -put /path/to/file /
		this will add the file to hdfs root directory. 
		*/
		val speeds = sc.textFile(filenameAndPath)
		// splitting lines by commas.
		.map(x => x.split(",").toList)
		// removing register out of date interval and outside given rectangle
		.filter(x => isdateInsideInterval(x(0)) && isInsideRectangle(x(3), x(4)))
		// transforming each rdd element into a float representing the register's speed.
		.map(_(5).toFloat)

		val total = speeds.fold(0)(_+_) // this calculates the sum of all elements in the rdd starting with 0.
		val amount = speeds.count // this counts how many elements exist in the rdd.
		val average = total / amount // average value

 		//not writing to hdfs yet...
		val pw = new PrintWriter(new File(resultFilenameAndPath), "UTF-8") // creating file to be written on.
		pw.write(args(0)+","+args(1)+","+args(2)+","+args(3)+","+args(4)+","+args(5)+"\n") // writing arguments on file.	
		pw.write(amount + "\n") // the amount of registers found
		pw.write(average + "\n") // writing result.
		pw.close // closing file.
		

		/* this will create the directory /result into hdfs and the files 
		will be inside it, one per worker. the file names will be part-00000, part-00001 and so on.
		if you run this code again with this line, it will try to write the same directory again and will give you error.
		delete the folder on hdfs before running it
		$ hadoop fs -rm -r /result 
		*/
	}
}