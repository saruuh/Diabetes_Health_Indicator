

DiabetesHealthIndicatorGroup_BigDataProject
IT462 – Big Data Systems
King Saud University

Project Overview
----------------
This project analyzes the CDC Diabetes Health Indicators dataset using Apache Spark and Scala.
The project includes:
- Data preprocessing
- RDD operations
- Spark SQL analysis
- Machine learning using Random Forest Classification

Dataset Source
--------------
CDC Diabetes Health Indicators Dataset
UCI Machine Learning Repository

https://archive.ics.uci.edu/dataset/891/cdc+diabetes+health+indicators

Folder Structure
----------------
DiabetesHealthIndicatorGroup_BigDataProject/

├── README.txt
├── FinalReport.pdf
├── Presentation_slides.pdf
│
├── code/
│   ├── 01_DataPreprocessing.scala
│   ├── 02_RDDOperations.scala
│   ├── 03_SQLOperations.scala
│   ├── 04_MachineLearning.scala
│   └── utility_functions.scala
│
├── data/
│   ├── raw_dataset.csv
│   └── preprocessed_dataset.csv
│
└── results/
    ├── rdd_output.txt
    ├── sql_results.csv
    └── ml_metrics.txt

Environment Setup
-----------------
Required Software:
- Java JDK 21
- Apache Spark 4.1.1
- Scala 2.13.17

Recommended Environment:
- macOS or Linux terminal
- VS Code or Brackets editor

Spark Version
-------------
Apache Spark 4.1.1

Scala Version
-------------
Scala 2.13.17

Dependencies
-------------
The project uses Spark built-in libraries only.

Main libraries:
- org.apache.spark.sql
- org.apache.spark.sql.functions
- org.apache.spark.ml
- org.apache.spark.ml.feature
- org.apache.spark.ml.classification
- org.apache.spark.ml.evaluation

No external dependencies are required.

How to Run the Preprocessing Script
-----------------------------------
1. Open terminal.
2. Navigate to the project folder:

cd ~/Downloads/DiabetesHealthIndicatorGroup_BigDataProject/code

3. Run the script:

spark-shell -i 01_DataPreprocessing.scala

The script performs:
- Missing value checking
- Duplicate removal
- Range validation
- Feature engineering
- Dataset transformation

Generated Output:
- preprocessed_dataset.csv

How to Run the RDD Operations Script
------------------------------------
1. Open terminal.
2. Navigate to the project folder:

cd ~/Downloads/DiabetesHealthIndicatorGroup_BigDataProject/code

3. Run the script:

spark-shell -i 02_RDDOperations.scala

The script performs:
- RDD transformations and actions
- Aggregation and filtering
- Diabetes prevalence analysis
- BMI analysis
- Risk factor analysis

Generated Output:
- rdd_output.txt

How to Run the SQL Operations Script
------------------------------------
1. Open terminal.
2. Navigate to the project folder:

cd ~/Downloads/DiabetesHealthIndicatorGroup_BigDataProject/code

3. Run the script:

spark-shell -i 03_SQLOperations.scala

The script performs:
- Spark SQL queries
- DataFrame operations
- Aggregation analysis
- Diabetes-related statistical analysis

Generated Output:
- sql_results.csv

How to Run the Machine Learning Script
--------------------------------------
1. Open terminal.
2. Navigate to the project folder:

cd ~/Downloads/DiabetesHealthIndicatorGroup_BigDataProject/code

3. Run the script:

spark-shell -i 04_MachineLearning.scala

The script performs:
- Feature assembly
- Label indexing
- Train/test split
- Random Forest classification
- Model evaluation
- Feature importance analysis

Generated Output:
- ml_metrics.txt

Expected Machine Learning Results
---------------------------------
Random Forest Accuracy:
Approximately 83.49%

Baseline Accuracy:
Approximately 82.82%

F1-Score:
Approximately 78.28%

Notes
-----
- The project uses the BRFSS 2015 dataset.
- All implementation was completed using Apache Spark and Scala.
- The machine learning task is formulated as a multi-class classification problem:
  0 = No Diabetes
  1 = Prediabetes
  2 = Diabetes
