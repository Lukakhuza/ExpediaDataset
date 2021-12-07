import org.apache.spark.sql._

object Run {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Reservations").master("local[4]").getOrCreate()
    val schema = Encoders.product[ColNames].schema
    val input = "/home/luka/Documents/Revature/files/input.csv"

    def processor = new Processor()

    // Run query 1
    println("Query1: What are the top 5 destinations?")
    val query1Result = processor.top5(spark, schema, input)

    // Run query 2
    println("Query 2: Top 5 Countries from which people make reservations:")
    val query2Result = processor.top5Origin(spark, schema, input)

    // Run query3
    println("Query 3: Average Distance Travelled by visitor:")
    val query3Result = processor.AverageDistance(spark, schema, input)

    // Run query4
    println("Query 4: What percentage of reservations were part of a package?")
    val query4Result = processor.partOfPackage(spark, schema, input)

    //      Run query5
    println("Query 5: What is the distribution of the number of reservations based on the number of people per reservation?")
    val query5Result = processor.PersonsPerReservationCount(spark, schema, input)

    // Run query 6
    println("Query 6: What is the expected number of reservations next month?")
    val query6Result = processor.ReservationsNextMonth(spark, schema,input)

  }
}
