//Create a DataFrame that lists employees with names and their work status. For each employee,
//determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
//within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
//  letter of each name is capitalized.

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, initcap, to_date, when}

object groupBy {
  def main(args:Array[String]):Unit=
    {
      val conf = new SparkConf()
        .setAppName("My Spark Application")       // Set the application name
        .setMaster("local[*]")                    // Set the master URL (local mode with all cores)
        .set("spark.executor.memory", "4g")       // Set the executor memory to 4 GB
        .set("spark.driver.memory", "2g")         // Set the driver memory to 2 GB
        .set("spark.executor.cores", "4")         // Set the number of executor cores to 4
        .set("spark.sql.shuffle.partitions", "500") // Set shuffle partitions to 500

      val spark = SparkSession.builder.config(conf).getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", "2024-11-01"),
        ("neha", "2024-10-20"),
        ("priya", "2024-10-28"),
        ("mohan", "2024-11-02"),
        ("ajay", "2024-09-15"),
        ("vijay", "2024-10-30"),
        ("veer", "2024-10-25"),
        ("aatish", "2024-10-10"),
        ("animesh", "2024-10-15"),
        ("nishad", "2024-11-01"),
        ("varun", "2024-10-05"),
        ("aadil", "2024-09-30")
      ).toDF("name", "last_checkin");
//
//      employees.show()

      val dfWithDate = employees
        .withColumn("last_checkin", to_date(col("last_checkin"), "yyyy-MM-dd"))

      val dfWithStatus = dfWithDate
        .withColumn("Status", when(datediff(current_date(), col("last_checkin")) <= 7, "Active")
          .otherwise("Inactive"))
      val finalDF = dfWithStatus.withColumn("Name", initcap(col("Name")))
      finalDF.show()


    }

}
