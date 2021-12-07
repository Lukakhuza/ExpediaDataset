import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

class Processor {

  // Query 1: Find top five most popular countries
  def top5(spark: SparkSession,
              schema: StructType,
              input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)
    val ds: Dataset[ColNames] = dr.as[ColNames]
    val result =
      ds.groupBy($"hotel_country")
      .count()
      .sort(desc("count"))
      .limit(5)
      result.show()
  }

  //Query 2: Find the top 5 Countries from which people make reservations
  def top5Origin(spark: SparkSession,
           schema: StructType,
           input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[ColNames] = dr.as[ColNames]

    val result =
//      ds.filter(p =>p.user_location_country.equalsIgnoreCase("2"))
        ds.groupBy($"user_location_country")
        .count()
        .sort(desc("count"))
        .limit(5)
    result.show()
  }

  // Query 3: Find average distance travelled by visitor
  def AverageDistance(spark: SparkSession,
                 schema: StructType,
                 input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[ColNames] = dr.as[ColNames]

    val result = {
      ds.filter("orig_destination_distance is not null")
        .select(mean("orig_destination_distance"))
    }
    result.show()
  }

  // Query 4: Find what percentage of reservations were part of a package
  def partOfPackage(spark: SparkSession,
                 schema: StructType,
                 input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[ColNames] = dr.as[ColNames]

    val result = {
      ds.filter(p=>p.is_package.equals("1"))
        .count()
    }
    val result1 = {
      ds.count()
    }

    val PackageTripsPercentage = result*100 / result1
    println("")
    println("")
    println("Percentage of Trips that were part of a package: " + PackageTripsPercentage + "%")
    println("")
    println("")
  }

  // Query 5: Find what is the distribution of the number of reservations based on the number of people per reservation
  def PersonsPerReservationCount(spark: SparkSession,
           schema: StructType,
           input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[ColNames] = dr.as[ColNames]

    val result =
      ds.groupBy($"srch_children_cnt")
        .count()
        .sort("srch_children_cnt")
        .limit(5)
    result.show()
  }

  //Query 6: Predict the number of reservations for next month
  def ReservationsNextMonth(spark: SparkSession,
                    schema: StructType,
                    input: String) = {
    import spark.implicits._
    val dr = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[ColNames] = dr.as[ColNames]

    val result1 = {
      ds.count()
    }
    val Total = result1.toInt
    val perMonth = Total / 8
    println("")
    println("")
    println("The Number of Reservations expected next month are: " + perMonth)
    println("")
    println("")
  }
}
