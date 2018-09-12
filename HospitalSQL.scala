package SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object HospitalSQL {
  val ManualSchemaHospital = new StructType(Array(new StructField("DRGDefinition", StringType,true),
    new StructField("ProviderId", LongType, false),
    new StructField("ProviderName", StringType, true),
    new StructField("ProviderStreetAddress", StringType, false),
    new StructField("ProviderCity", StringType, false),
    new StructField("ProviderState", StringType, false),
    new StructField("ProviderZipCode", LongType, false),
    new StructField("HospitalReferralRegionDescription", StringType, true),
    new StructField("TotalDischarges", LongType, false),
    new StructField("AverageCoveredCharges", DoubleType, false),
    new StructField("AverageTotalPayments", DoubleType, false),
    new StructField("AverageMedicarePayments", DoubleType, false)))

  def main(args: Array[String]): Unit = {
    println("hey Scala, this is Hospital Use Case Session")

    //Let us create a spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Hospital Use Case")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    println("Spark Session Object Created")

    //Load data from the CSV file
    val HospitalData = spark.read.format("csv")
      .option("header", "true")
      .schema(ManualSchemaHospital)
      .load("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Hospital\\inpatientCharges.csv")
      .toDF()
    println("\nInpatient Hospital Data ->> "+HospitalData.count())
    println("Hospital Data Loaded and Displayed!")
    HospitalData.show()

    //Create a view
    HospitalData.createOrReplaceTempView("HospitalView")
    spark.sql("""select ProviderState, round(avg(AverageCoveredCharges),2)
        |as AvgCoverageCharges_State from HospitalView
        |Group by ProviderState""".stripMargin)
      .show()
    println("HospitalView Created and Displayed!\n\n")

    //Average Total Payments State wise
    spark.sql("""select ProviderState, round(sum(cast(AverageTotalPayments as decimal)/cast(pow(10,2) as decimal)),2)
        |as AvgTotPayment_State from HospitalView
        |Group by ProviderState""".stripMargin)
      .show()
    println("State wise Average Total Payments Calculated and Displayed!\n\n")

    //Average Medicare Payments State wise
    spark.sql("""select ProviderState, round(sum(cast(AverageMedicarePayments as decimal)/cast(pow(10,2) as decimal)),2)
        |as AvgMediPayment_State from HospitalView
        |Group by ProviderState""".stripMargin)
      .show()
    println("State wise Average Medicare Payments Calculated and Displayed!\n\n")

    //Total number of Discharges per State and for each disease
    //Sort the output in descending order of totalDischarges
    spark.sql("""select DRGDefinition,ProviderState, sum(TotalDischarges) as TotalDischarged from HospitalView
        |Group by DRGDefinition, ProviderState
        |Order by TotalDischarged desc""".stripMargin)
      .show()
    println("State wise Total Discharges made for each disease calculated and displayed in descending!\n\n")
  }
}