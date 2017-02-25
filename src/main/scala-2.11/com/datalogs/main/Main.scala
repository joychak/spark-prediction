package com.datalogs.main

import java.io.File

import com.datalogs.classification.{MLModels, Metrics}
import com.datalogs.features.FeatureConstruction
import com.datalogs.utils.{DataLoad, PlotUtils}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

/**
  * Created by joychak on 1/7/17.
  */
object Main {

  class RunConf(args: Array[String]) extends ScallopConf(args) {

    val csvPath = opt[String]("csv-dir", required = true)
    val featurePath = opt[String]("feature-dir", required = true)
    val phase = opt[String]("pipeline-stage", required = true).map(_.toInt)
    verify()
  }

  def main(args: Array[String]): Unit = {

    val conf = new RunConf(args)

    val (sc, spark) = createContext

    conf.phase() match {
      case 1 => {
        constructFeatures(sc, spark, conf)
      }
      case 2 => {
        runModels(spark, conf.featurePath())
      }
      case 3 => {
        constructFeatures(sc, spark, conf)
        runModels(spark, conf.featurePath())
      }
    }
  }

  def constructFeatures(sc: SparkContext, spark: SparkSession, conf: RunConf) = {

    val (patients, diags, meds, labs, events) = DataLoad.loadPatientData(spark, conf.csvPath())
    println(s"Patient data load finished")

    //Returns => List of (Patient_ID, Index_Date)
    val indexDates = FeatureConstruction.cobstructPatientIndexDate(spark, events, patients)
    println(s"${indexDates.count} patient index date calculation finished")

    //Returns => List of {(Patient_ID, Diag_Code), Value}
    val diagnosticFeature = FeatureConstruction.constructDiagnosticFeatureTuple(spark, indexDates, diags)
    println(s"${diagnosticFeature.count} diagnostic feature construction finished")

    //Returns => List of {(Patient_ID, Drug), Value}
    val medicationFeature = FeatureConstruction.constructMedicationFeatureTuple(spark, indexDates, meds)
    println(s"${medicationFeature.count} medication feature construction finished")

    //Returns => List of {(Patient_ID, Lab), Value}
    val labResultFeature =  FeatureConstruction.constructLabResultFeatureTuple(spark, indexDates, labs)
    println(s"${labResultFeature.count} lab result feature construction finished")

    //Returns => List of {(PATIENT-ID, EVENT-CODE), VALUE}
    val allFeature = diagnosticFeature.union(medicationFeature).union(labResultFeature)

    FileUtils.deleteQuietly(new File(conf.featurePath()))

    //Construct Feature Vector and saves it to a file system
    //===================== Structure ===========================================
    // Patient-1 isDead  Event-1:Val-1 Event2:val2 Event2:val3 :::::: Eventn:valn
    // Patient-2 isDead  Event-1:Val-1 Event2:val2 Event2:val3 :::::: Eventn:valn
    //      ::::::::::::::::::::::::::
    // Patient-N isDead  Event-1:Val-1 Event2:val2 Event2:val3 :::::: Eventn:valn
    //===========================================================================
    FeatureConstruction.constructFeatureVector(sc, patients.rdd, allFeature, conf.featurePath())
    println(s"Feature construction finished")
  }

  def runModels(spark: SparkSession, featurePath: String) = {
    runOneModel(spark, featurePath, "SVM")
    runOneModel(spark, featurePath, "LR")
    runOneModel(spark, featurePath, "DT")
  }

  def runOneModel(spark: SparkSession, featurePath: String, model: String): Unit = {

    val featureInput = featurePath + "/part-*"

    val results = MLModels.run(spark, featureInput, model)
    val (aucROC, roc) = Metrics.getMetrics(results)

    println("======================")
    println(model + " Area under ROC = " + aucROC)
    println("**********************")

    PlotUtils.plot(roc, aucROC, model + s": ${"%.3f".format(aucROC)}")
  }

  def createContext: (SparkContext, SparkSession) = {
    val conf = new SparkConf()  //.setAppName(appName).setMaster(masterUrl).set("spark.executor.memory", "2g")
    (new SparkContext(conf), SparkSession.builder().config(conf).getOrCreate())
  }
}
