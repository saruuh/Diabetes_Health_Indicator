import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val path = "/Users/dalalalyousef/Downloads/IT462_Project/data/diabetes_012_health_indicators_BRFSS2015.csv"

val rawDF = spark.read
  .option("header","true")
  .option("inferSchema","true")
  .csv(path)

rawDF.columns.map(c => (c, rawDF.filter(col(c).isNull).count())).foreach(println)

val beforeDup = rawDF.count()

val dedupDF = rawDF.dropDuplicates()

val afterDup = dedupDF.count()

println(beforeDup)
println(afterDup)

val beforeClean = dedupDF.count()

val cleanedDF = dedupDF
  .filter(col("BMI") > 10 && col("BMI") < 80)
  .filter(col("MentHlth") >= 0 && col("MentHlth") <= 30)
  .filter(col("PhysHlth") >= 0 && col("PhysHlth") <= 30)

val afterClean = cleanedDF.count()

println(beforeClean)
println(afterClean)

val integratedDF = cleanedDF
  .withColumnRenamed("Diabetes_012", "Diabetes")
  .withColumn("Diabetes", col("Diabetes").cast(IntegerType))

integratedDF.printSchema()

val beforeCols = integratedDF.columns.length

val reducedDF = integratedDF.drop("CholCheck")

val afterCols = reducedDF.columns.length

println(beforeCols)
println(afterCols)

val transformedDF = reducedDF
  .withColumn("BMI_Category",
    when(col("BMI") < 18.5, 0)
      .when(col("BMI") < 25, 1)
      .when(col("BMI") < 30, 2)
      .otherwise(3)
  )
  .withColumn("UnhealthyLifestyleScore",
    col("Smoker") + col("HvyAlcoholConsump") + (lit(1) - col("PhysActivity"))
  )

transformedDF
  .coalesce(1)
  .write
  .mode("overwrite")
  .option("header","true")
  .csv("/Users/dalalalyousef/Downloads/IT462_Project/data/preprocessed_diabetes")

transformedDF
  .limit(20)
  .coalesce(1)
  .write
  .mode("overwrite")
  .option("header","true")
  .csv("/Users/dalalalyousef/Downloads/IT462_Project/data/Phase2_snapshot")