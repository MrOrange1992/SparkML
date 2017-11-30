package MedienTransparenz

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{stddev_pop, stddev_samp}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD



object MediaAnalysisLinearRegression
{

  def main(args: Array[String]): Unit =
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Context, using all Cores of local machine
    val spark = SparkSession.builder.appName("MediaAnalysisLinearRegression").master("local[*]").getOrCreate()


    //Custom Schema for data frame to cast numeric fields
    val customSchema = StructType(Array(
      StructField("RECHTSTRÄGER", StringType, true),
      StructField("QUARTAL", IntegerType, true),
      StructField("BEKANNTGABE", IntegerType, true),
      StructField("LEERMELDUNG", IntegerType, true),
      StructField("MEDIUM_MEDIENINHABER", StringType, true),
      StructField("EURO", FloatType, true)
    ))

    /*
     csv sanitations ~lukas:
      deleted all unused columns
      replaced all 'null' with ''
      replaced all '""' with ''
   */
    val data = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(customSchema)
      .load("./data/MedienTransparenz-20123-20171.csv")
      .cache()
    //data.printSchema()
    //data.show()

    //Filter null fields
    //BAKANNTGABE:
      //4: Förderungen und Programmentgeld
      //2: Bekanntgabepflichtige Aufträge
    val filteredData = data.filter(data("LEERMELDUNG") === "0").filter(data("BEKANNTGABE") =!= "4")

    val quarterGroups = filteredData.groupBy("QUARTAL")

    //Mittelwert für Ausgaben im Quartal gesamt
    //quarterGroups.mean("EURO").sort("QUARTAL").show()

    //Standardabweichung der Ausgaben pro Quartal
    //filteredData.groupBy("QUARTAL").agg(stddev_pop("EURO")).sort("QUARTAL").show()//.describe().show()


    val countRT = filteredData.groupBy("RECHTSTRÄGER").count()


    val groupedRT = countRT.filter(countRT("count") > 10)


    val keySet = filteredData.select("RECHTSTRÄGER").groupBy("RECHTSTRÄGER").count()//.distinct.collect//.flatMap(_.toSeq)

    //val groupedMedia = filteredData.groupBy("MEDIUM_MEDIENINHABER")

    val mediaRDD =  filteredData.rdd


    // FRAGE
    val mediaMap = mediaRDD.map( r => (r(0), List((r(4), r(5))))).reduceByKey((a, b) => a ++ b)//if (a.head._1 == b.head._1) a else a ++ b)


    val pairs = mediaRDD.map(s => (s(0), 1))



    //val mediaMap = pairs.reduceByKey((x,y) => x - y)



    //pairs foreach println

    //println(mediaMap.count())


    //mediaRDD foreach println

    type mediaMap = RDD[(String, List[(String, Float)])]

    /*

    // select desired fields and create label column in DS
    val mtDataDF = filteredData.select(data("QUARTAL").cast(FloatType).as("label"), data("EURO").cast(FloatType))


    // features column must be of type VectorAssembler
    // in our case it is just one field ("EURO)
    val assembler = new VectorAssembler().setInputCols(Array("EURO")).setOutputCol("features")

    // prepare dataframe
    val dataLR = assembler.transform(mtDataDF).select("label", "features")

    // LINEAR REGRESSION
    val lr = new LinearRegression()

    //model
    val lrModel = lr.fit(dataLR)

    //MODEL SUMMARY
    println(s"Coefficients: ${lrModel.coefficients} \nIntercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    */

  }

}


