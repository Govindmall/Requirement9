import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement9{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //group by country, sum Total Revenue
    val salesDF1=salesDF.coalesce(2)
    val countrySalesDF=salesDF1.groupBy("Country").agg(sum("Total Revenue").as("Total_Sales")).persist()
    //find the country with highest total sales
    val topCountryDF=countrySalesDF.orderBy(desc("Total_Sales")).limit(1)

    topCountryDF.show()
    topCountryDF.write.parquet("C:/Users/gomall/Desktop/requirement9.parquet")
    spark.stop()


  }

}