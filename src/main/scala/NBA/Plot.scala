package NBA

import co.theasi.plotly
import co.theasi.plotly.{Plot, writer}
import co.theasi.plotly._

class Plot
{
  //PLOTLY
  //------------------------------------------------------------------------------------------------------------------


  // Options common to traces
  val commonOptions: ScatterOptions = ScatterOptions().mode(ScatterMode.Marker).marker(MarkerOptions().size(8).lineWidth(1))


  def makeScattePlotSingleTrace(plotName:String, xRange:Range, data:Array[Double], label:String): Unit=
  {
    val plot = Plot().withScatter(xRange, data, commonOptions.name(label))
    draw(plot, plotName, writer.FileOptions(overwrite=true))
  }

  def makeScattePlotDoubleTrace(plotName:String, xRange:Range, data1:Array[Double], label1:String, data2:Array[Double], label2:String): Unit =
  {
    val plot = Plot()
      .withScatter(xRange, data1, commonOptions.name(label1))
      .withScatter(xRange, data2, commonOptions.name(label2))
    draw(plot, plotName, writer.FileOptions(overwrite=true))
  }


  //val plotData = predictions.rdd.toDS()//.sort($"_1").withColumn("label", $"_1").withColumn("prediction", $"_2")

  //plotData.describe().show()

  //val evalFrame = predictions.filter(predictions("label") === 1f && predictions("prediction") >= 0.5)//randomSplit(Array(0.5, 0.5))(0)

  //val xs = 0 until 500


  //implicit val y1: Array[Double] = evalFrame.select($"label").rdd.map(_(0).toString.toDouble).collect().take(500)
  //implicit val y2: Array[Double] = evalFrame.select($"prediction").rdd.map(_(0).toString.toDouble).collect().take(500)





/*
  // The plot itself
  val plot = Plot()
    .withScatter(xs, y1, commonOptions.name("Label"))
    .withScatter(xs, y2, commonOptions.name("Prediction"))



  draw(plot, "FD_LinReg_ML", writer.FileOptions(overwrite=true))
  */


}
