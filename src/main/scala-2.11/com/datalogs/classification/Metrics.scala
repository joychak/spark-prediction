package com.datalogs.classification

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD

/**
  * Created by joychak on 1/9/17.
  */
object Metrics {
  def getMetrics(scoreAndLabels: RDD[(Double, Double)]): (Double, RDD[(Double, Double)]) = {
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val roc = metrics.roc()

    (auROC, roc)
  }
}
