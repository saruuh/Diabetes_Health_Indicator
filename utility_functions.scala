import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object UtilityFunctions {

  def printSection(title: String): Unit = {
    println()
    println("=" * 60)
    println(title)
    println("=" * 60)
  }

  def printRowCount(df: DataFrame, label: String): Unit = {
    println(s"$label: ${df.count()}")
  }

  def printColumnCount(df: DataFrame, label: String): Unit = {
    println(s"$label: ${df.columns.length}")
  }

  def checkNullValues(df: DataFrame): Unit = {
    df.columns
      .map(c => (c, df.filter(col(c).isNull).count()))
      .foreach(println)
  }

  def showSample(df: DataFrame, rows: Int): Unit = {
    df.show(rows, false)
  }

  def saveCSV(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }

  def printRDDCount(rdd: RDD[Array[Double]], label: String): Unit = {
    println(s"$label: ${rdd.count()}")
  }

  def calculatePercentage(count: Long, total: Long): Double = {
    count.toDouble / total * 100
  }

  def printMetric(name: String, value: Double): Unit = {
    println(f"$name%-25s = $value%.4f")
  }

  def printFeatureImportance(feature: String, importance: Double): Unit = {
    println(s"$feature -> $importance")
  }

}