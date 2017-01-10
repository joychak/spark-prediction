package com.datalogs.classification

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by joychak on 1/9/17.
  */
object MLModels {
  /**
    * Run with 70-30 % split of the input dataset
    *
    * @param spark
    * @param path
    * @return
    */
  def run(spark: SparkSession, path: String, model: String): RDD[(Double, Double)] = {

    val data = MLUtils.loadLibSVMFile(spark.sparkContext, path)

    val scaler = new StandardScaler().fit(data.map(x => x.features))
    //val normalizer = new Normalizer()

    val scaledData = data
      .map(x => (x.label, scaler.transform(x.features)))
      //.map(x => (x._1, normalizer.transform(x._2)))
      .map(x => LabeledPoint(x._1, x._2))

    val splits = scaledData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    model match {
      case "SVM" => runSVM(training, test)
      case "LR" => runLR(training, test)
      case "DT" => runDT(training, test)
    }
  }

  /**
    * Run SVM
    *
    * @param training
    * @param test
    * @return
    */
  def runSVM(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {
    // Run training algorithm to build the model
    val numIterations = 1000
    val model = SVMWithSGD
      .train(training, numIterations)
      //.setThreshold(0.5)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      //println(s"label: ${point.label}; score: $score%1.5f")
      (score, point.label)
    }
    scoreAndLabels
  }

  /**
    * Run Logistic regression
    *
    * @param training
    * @param test
    * @return
    */
  def runLR(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {

    val lr = new LogisticRegressionWithLBFGS()
    lr.optimizer.setNumIterations(10000).setRegParam(0.0001)

    val model = lr
      .setNumClasses(2)
      .run(training)
      .clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      //println(f"label: ${point.label}; score: $score%1.5f")
      (score, point.label)
    }
    scoreAndLabels
  }

  def runDT(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[(Double, Double)]  = {

    // Run training algorithm to build the model
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    scoreAndLabels
  }
}
