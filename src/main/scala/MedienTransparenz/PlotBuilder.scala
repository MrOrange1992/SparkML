package MedienTransparenz

import org.apache.spark.sql

class PlotBuilder
{

  def saveCSV(data: sql.DataFrame, path: String) = data.write.format("csv").save(path)


}
