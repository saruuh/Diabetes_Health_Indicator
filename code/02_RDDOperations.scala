sc.setLogLevel("ERROR")

// ─────────────────────────────────────────────
// Load dataset
// ─────────────────────────────────────────────
val filePath = "file:///Users/sara/Desktop/preprocessed_diabetes.csv"

val rawRDD = sc.textFile(filePath)

// Remove header
val header = rawRDD.first()
val dataRDD = rawRDD.filter(line => line != header)

// Parse rows into arrays of Double
val parsedRDD = dataRDD.map { line =>
  line.split(",").map(_.trim.toDouble)
}.cache()

println("\n" + "=" * 60)
println("  IT462 - Phase 3: Diabetes RDD Analysis")
println("=" * 60 + "\n")

// ─────────────────────────────────────────────
// Column Indices
// ─────────────────────────────────────────────
val DIABETES = 0
val HIGH_BP = 1
val HIGH_CHOL = 2
val BMI = 3
val SMOKER = 4
val STROKE = 5
val HEART_DISEASE = 6
val PHYS_ACTIVITY = 7
val FRUITS = 8
val VEGGIES = 9
val HVY_ALCOHOL = 10
val HEALTHCARE = 11
val NO_DOCTOR_COST = 12
val GEN_HLTH = 13
val MENT_HLTH = 14
val PHYS_HLTH = 15
val DIFF_WALK = 16
val SEX = 17
val AGE = 18
val EDUCATION = 19
val INCOME = 20
val BMI_CATEGORY = 21
val UNHEALTHY_SCORE = 22

// ─────────────────────────────────────────────
// 2.1 Dataset Size Verification
// ─────────────────────────────────────────────
println("─" * 60)
println("2.1 Dataset Size Verification")
println("─" * 60)

val totalCount = parsedRDD.count()

println(s"Total number of records: $totalCount")
println()

// ─────────────────────────────────────────────
// 2.2 Data Validation Sample
// ─────────────────────────────────────────────
println("─" * 60)
println("2.2 Data Validation Sample")
println("─" * 60)

val firstRecord = parsedRDD.first()

println("First record:")
println(firstRecord.mkString(", "))
println()

// ─────────────────────────────────────────────
// 2.3 Diabetes Distribution by Age Group
// ─────────────────────────────────────────────
println("─" * 60)
println("2.3 Diabetes Distribution by Age Group")
println("─" * 60)

val ageGroupRDD = parsedRDD.map { fields =>

  val age = fields(AGE).toInt
  val diabetes = fields(DIABETES).toInt

  (age, (diabetes, 1))
}

val ageGroupTotals = ageGroupRDD
  .reduceByKey(
    (a: (Int, Int), b: (Int, Int)) =>
      (a._1 + b._1, a._2 + b._2)
  )
  .mapValues { value =>

    val diabeticCount = value._1
    val total = value._2

    val rate = diabeticCount.toDouble / total * 100

    (diabeticCount, total, rate)
  }
  .sortBy(_._1)

println("Age Group | Diabetes Cases | Total | Diabetes Rate (%)")
println("-" * 60)

ageGroupTotals.collect().foreach {

  case (age, (diabCount, total, rate)) =>

    println(
      f"Age $age%2d | $diabCount%6d | $total%6d | $rate%6.2f%%"
    )
}

println()

// ─────────────────────────────────────────────
// 2.4 Average BMI by Diabetes Status
// ─────────────────────────────────────────────
println("─" * 60)
println("2.4 Average BMI by Diabetes Status")
println("─" * 60)

val bmiByDiabetes = parsedRDD
  .map { fields =>

    val diabetes = fields(DIABETES).toInt
    val bmi = fields(BMI)

    (diabetes, (bmi, 1))
  }
  .reduceByKey(
    (a: (Double, Int), b: (Double, Int)) =>
      (a._1 + b._1, a._2 + b._2)
  )
  .mapValues { value =>

    val totalBMI = value._1
    val count = value._2

    totalBMI / count
  }

println("Diabetes Status         | Average BMI")
println("-" * 45)

bmiByDiabetes.collect().sortBy(_._1).foreach {

  case (status, avgBMI) =>

    val label =
      status match {
        case 0 => "No Diabetes"
        case 1 => "Prediabetes"
        case 2 => "Diabetes"
        case _ => "Unknown"
      }

    println(f"$label%-25s | $avgBMI%.2f")
}

println()

// ─────────────────────────────────────────────
// 2.5 Identification of High-Risk Individuals
// ─────────────────────────────────────────────
println("─" * 60)
println("2.5 Identification of High-Risk Individuals")
println("─" * 60)

// High-risk criteria:
// HighBP = 1
// Smoker = 1
// PhysActivity = 0
// GenHlth >= 3

val highRiskRDD = parsedRDD.filter { fields =>

  val highBP = fields(HIGH_BP).toInt
  val smoker = fields(SMOKER).toInt
  val physActivity = fields(PHYS_ACTIVITY).toInt
  val genHlth = fields(GEN_HLTH).toInt

  highBP == 1 &&
  smoker == 1 &&
  physActivity == 0 &&
  genHlth >= 3
}

val highRiskCount = highRiskRDD.count()

val highRiskPct =
  highRiskCount.toDouble / totalCount * 100

println(s"High-risk individuals count: $highRiskCount")
println(f"Percentage of total population: $highRiskPct%.2f%%")

println()

// ─────────────────────────────────────────────
// 2.6 BMI Category Distribution
// ─────────────────────────────────────────────
println("─" * 60)
println("2.6 BMI Category Distribution")
println("─" * 60)

val bmiCategoryLabels = Map(
  0 -> "Underweight",
  1 -> "Normal",
  2 -> "Overweight",
  3 -> "Obese"
)

val bmiDist = parsedRDD
  .map { fields =>

    val bmiCategory =
      fields(BMI_CATEGORY).toInt

    (bmiCategory, 1)
  }
  .reduceByKey(
    (a: Int, b: Int) => a + b
  )
  .sortBy(_._2, ascending = false)

println("BMI Category | Count | Percentage")
println("-" * 45)

bmiDist.collect().foreach {

  case (category, count) =>

    val label =
      bmiCategoryLabels.getOrElse(
        category,
        "Unknown"
      )

    val pct =
      count.toDouble / totalCount * 100

    println(
      f"$label%-15s | $count%6d | $pct%6.2f%%"
    )
}

println()

// ─────────────────────────────────────────────
// 2.7 General Health Distribution
// ─────────────────────────────────────────────
println("─" * 60)
println("2.7 General Health Distribution")
println("─" * 60)

val genHlthLabels = Map(
  1 -> "Excellent",
  2 -> "Very Good",
  3 -> "Good",
  4 -> "Fair",
  5 -> "Poor"
)

val genHlthDist = parsedRDD
  .map { fields =>

    val genHlth =
      fields(GEN_HLTH).toInt

    (genHlth, 1)
  }
  .reduceByKey(
    (a: Int, b: Int) => a + b
  )
  .sortByKey()

println("GenHlth | Label      | Count | Percentage")
println("-" * 55)

genHlthDist.collect().foreach {

  case (score, count) =>

    val label =
      genHlthLabels.getOrElse(
        score,
        "Unknown"
      )

    val pct =
      count.toDouble / totalCount * 100

    println(
      f"$score%7d | $label%-10s | $count%6d | $pct%6.2f%%"
    )
}

println()

// ─────────────────────────────────────────────
// 2.8 Top Age Groups by Diabetes Prevalence
// ─────────────────────────────────────────────
println("─" * 60)
println("2.8 Top Age Groups by Diabetes Prevalence")
println("─" * 60)

val topAgeGroups =
  ageGroupTotals.sortBy(
    x => x._2._3,
    ascending = false
  )

println("Rank | Age Group | Diabetes Rate | Cases")
println("-" * 55)

topAgeGroups
  .take(5)
  .zipWithIndex
  .foreach {

    case (
      (
        age,
        (
          diabCount,
          total,
          rate
        )
      ),
      index
    ) =>

      println(
        f"${index + 1}%4d | Age $age%2d | $rate%6.2f%% | $diabCount/$total"
      )
  }

println()

println("=" * 60)
println("Phase 3 Analysis Complete")
println("=" * 60)
System.getProperty("user.dir")
