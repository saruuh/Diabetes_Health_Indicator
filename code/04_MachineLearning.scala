import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline

val data = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .csv("/Users/dalalalyousef/Downloads/IT462_Project/data/preprocessed_diabetes/preprocessed_dataset.csv")

val selectedData = data.select(
  "Diabetes",
  "HighBP",
  "HighChol",
  "BMI",
  "Smoker",
  "Stroke",
  "HeartDiseaseorAttack",
  "PhysActivity",
  "Fruits",
  "Veggies",
  "HvyAlcoholConsump",
  "AnyHealthcare",
  "NoDocbcCost",
  "GenHlth",
  "MentHlth",
  "PhysHlth",
  "DiffWalk",
  "Sex",
  "Age",
  "Education",
  "Income"
)

val labelIndexer = new StringIndexer()
  .setInputCol("Diabetes")
  .setOutputCol("label")

val featureColumns = Array(
  "HighBP",
  "HighChol",
  "BMI",
  "Smoker",
  "Stroke",
  "HeartDiseaseorAttack",
  "PhysActivity",
  "Fruits",
  "Veggies",
  "HvyAlcoholConsump",
  "AnyHealthcare",
  "NoDocbcCost",
  "GenHlth",
  "MentHlth",
  "PhysHlth",
  "DiffWalk",
  "Sex",
  "Age",
  "Education",
  "Income"
)

val assembler = new VectorAssembler()
  .setInputCols(featureColumns)
  .setOutputCol("features")

val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(50)
  .setMaxDepth(10)
  .setSeed(42)

val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, assembler, rf))

val Array(trainingData, testData) = selectedData.randomSplit(Array(0.7, 0.3), seed = 42)

val model = pipeline.fit(trainingData)

val predictions = model.transform(testData)

val accuracyEvaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val f1Evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("f1")

val accuracy = accuracyEvaluator.evaluate(predictions)
val f1Score = f1Evaluator.evaluate(predictions)

println("Random Forest Model Performance")
println(s"Accuracy = $accuracy")
println(s"F1-Score = $f1Score")

val majorityClass = trainingData.groupBy("Diabetes")
  .count()
  .orderBy(desc("count"))
  .first()
  .get(0)

vval baselinePredictions = testData.withColumn(
  "prediction",
  lit(majorityClass).cast("double")
)

val indexedBaseline = labelIndexer.fit(selectedData).transform(baselinePredictions)

val baselineAccuracy = accuracyEvaluator
  .setPredictionCol("prediction")
  .evaluate(indexedBaseline.withColumn("label", col("label").cast("double")))

println(s"Baseline Accuracy = $baselineAccuracy")

val rfModel = model.stages.last
  .asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]

val importances = rfModel.featureImportances.toArray

val featureImportanceDF = featureColumns.zip(importances)
  .sortBy(-_._2)

println("Feature Importances")

featureImportanceDF.foreach {
  case (feature, importance) =>
    println(s"$feature -> $importance")
}

predictions.select("Diabetes", "prediction", "probability").show(10, false)