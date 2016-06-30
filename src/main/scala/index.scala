import org.apache.spark.{SparkContext, SparkConf}
import java.io._
import java.util.Date
import java.text.SimpleDateFormat

object AverageSpeed {
	
	val conf = new SparkConf().setAppName("AverageSpeed")
    val sc = new SparkContext(conf)

    // path to files being read.
	val filenameAndPath = "hdfs://localhost:8020/riobusData/estudo_cassio_part_00000000000[0-19]*"

	val dateFormatGoogle = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") // format used by the data we have.
	val dateFormathttp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss") // format we use inside http message.

	def main(args: Array[String]) {
		val resultFilenameAndPath = args(0) // path to file where spark driver should write results to.

		val dateBegin = dateFormathttp.parse(args(1)) // value date that will hold the date interval's beginning.
		val dateEnd = dateFormathttp.parse(args(2)) // value date that will hold the date interval's end.

		// rectangle specified by an anchor point, a length and a height. the anchor point is in the top left corner.
		val latitude1 = args(3).toDouble // value date that will hold the rectangle's bottom left latitude.
		val longitude1 = args(4).toDouble // value date that will hold the rectangle's bottom left longitude.
		val latitude2 = args(5).toDouble // value date that will hold rectangle's top right latitude.
		val longitude2 = args(6).toDouble // value date that will hold rectangle's top right longitude.

		// returns true if 'stringDate', converted to Date object is bigger than 'dateBegin' and smaller than 'dateEnd'.
		val isdateInsideInterval = {(stringDate: String) =>
			//converting string to a date using pattern inside 'dateFormatGoogle'.
			var date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(stringDate)
			/* apparently I can't use the 'dateFormatGoogle' because 'SimpleDateFormat' is not thread safe. I need a
			new instance for every thread, but I don't know how to do it in an spark application. That's why I create a
			new instance inside every function call. */
			date.compareTo(dateBegin) >= 0 && date.compareTo(dateEnd) <= 0 // testing if date is inside date interval.
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
			// transforming each rdd element into a 2-touple (speed, 1) that represents each speed weighted by 1.
			.map(x => (x(5).toFloat, 1))
			// the sum of all rdd elements. first touple position summed together and same for second position
			.fold((0, 0))((x, y) => (x._1 + y._1, x._2 + y._2))
			// touple's first position is the sum of all speeds. second position is the amount of register found.
		
		// we will need to write in a file the argument we have received (as a confirmation) and the result.
		val pw = new PrintWriter(new File(resultFilenameAndPath), "UTF-8") // creating file to be written on.
		// writing the arguments we have received. just to give a feedback.
		pw.write(args(1)+","+args(2)+","+args(3)+","+args(4)+","+args(5)+","+args(6)+ "\n")
		// writing the result.
		pw.write(speeds._1/speeds._2 + "\n")
		pw.close // closing file.
	}
}