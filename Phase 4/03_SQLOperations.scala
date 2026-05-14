val diabetesDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/Users/jood/Downloads/diabetes_preprocessed.csv")

diabetesDF.createOrReplaceTempView("diabetes_data")

// Query 1: Diabetes Rate by Age Group
spark.sql("""
SELECT 
    Age,
    COUNT(*) AS total_individuals,
    SUM(CASE WHEN Diabetes > 0 THEN 1 ELSE 0 END) AS diabetic_individuals,
    ROUND(
        SUM(CASE WHEN Diabetes > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS diabetes_rate
FROM diabetes_data
GROUP BY Age
ORDER BY Age
""").show()

// Query 2: Average BMI by Diabetes Status
spark.sql("""
SELECT 
    Diabetes,
    COUNT(*) AS total_individuals,
    ROUND(AVG(BMI), 2) AS average_bmi
FROM diabetes_data
GROUP BY Diabetes
ORDER BY Diabetes
""").show()

// Query 3: High-Risk Individuals Count
spark.sql("""
SELECT 
    COUNT(*) AS high_risk_individuals
FROM diabetes_data
WHERE HighBP = 1
  AND Smoker = 1
  AND PhysActivity = 0
  AND Fruits = 0
  AND Veggies = 0
""").show()

// Query 4: Top Age Brackets by Diabetes Prevalence
spark.sql("""
SELECT 
    Age,
    COUNT(*) AS total_individuals,
    SUM(CASE WHEN Diabetes > 0 THEN 1 ELSE 0 END) AS diabetic_individuals,
    ROUND(
        SUM(CASE WHEN Diabetes > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS diabetes_rate
FROM diabetes_data
GROUP BY Age
ORDER BY diabetes_rate DESC
LIMIT 5
""").show()

// Query 5: BMI Category Distribution
spark.sql("""
SELECT 
    CASE 
        WHEN BMI < 18.5 THEN 'Underweight'
        WHEN BMI >= 18.5 AND BMI < 25 THEN 'Normal'
        WHEN BMI >= 25 AND BMI < 30 THEN 'Overweight'
        ELSE 'Obese'
    END AS bmi_category,
    COUNT(*) AS total_individuals
FROM diabetes_data
GROUP BY 
    CASE 
        WHEN BMI < 18.5 THEN 'Underweight'
        WHEN BMI >= 18.5 AND BMI < 25 THEN 'Normal'
        WHEN BMI >= 25 AND BMI < 30 THEN 'Overweight'
        ELSE 'Obese'
    END
ORDER BY total_individuals DESC
""").show()
