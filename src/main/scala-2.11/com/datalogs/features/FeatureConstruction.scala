package com.datalogs.features

import com.datalogs.datamodels._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Date

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by joychak on 1/7/17.
  */
object FeatureConstruction {

  /**
    * ((patient-id, feature-name), feature-value)
    */
  type FeatureTuple = ((String, String), Double)

  val obervationWindow = 2000L * 24 * 60 * 60 * 1000
  val indexDateDiffForDeadPatient = 30L * 24 * 60 * 60 * 1000

  def cobstructPatientIndexDate(spark: SparkSession, events: Dataset[PatientEvent], patients: Dataset[Patient])
    : RDD[(String, Date)] = {

    val deadPatients = patients.rdd
      .filter(x => x.isDead == 1.0)
      .map(x => (x.patientID, x.dod))
      .collectAsMap()

    val scDeadPatientMap = spark.sparkContext.broadcast(deadPatients)

    events.rdd
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.timestamp.after(y.timestamp)) x else y)
      .map {case (patientId, lastEvent) =>

        //calculate the index date for dead patient by subtracting 30 days from mortality date
        if (scDeadPatientMap.value.contains(patientId)) {
          (patientId, scDeadPatientMap.value.find(x => x._1 == patientId).get._2)
        } else {
          (patientId, lastEvent.timestamp)
        }
      }
  }

  def constructDiagnosticFeatureTuple(spark: SparkSession, patients: RDD[(String, Date)],
                                      diagnostic: Dataset[Diagnostic]): RDD[FeatureTuple] = {

    val scPatientMap = spark.sparkContext.broadcast(patients.collectAsMap)

    diagnostic.rdd
      .filter(diag => {
        val patient = scPatientMap.value.find(x => x._1 == diag.patientID).get

        patient._2.after(diag.date) && (patient._2.getTime - diag.date.getTime) <= obervationWindow
      })
      .map(x => ((x.patientID, x.code), 1.0))
      .reduceByKey(_+_)
  }

  def constructMedicationFeatureTuple(spark: SparkSession, patients: RDD[(String, Date)],
                                      medication: Dataset[Medication]): RDD[FeatureTuple] = {

    val scPatientMap = spark.sparkContext.broadcast(patients.collectAsMap)

    medication.rdd
      .filter(med => {
        val patient = scPatientMap.value.find(x => x._1 == med.patientID).get
        patient._2.after(med.date) && (patient._2.getTime - med.date.getTime) <= obervationWindow
      })
      .map(x => ((x.patientID, x.medicine), 1.0))
      .reduceByKey(_+_)
  }

  def constructLabResultFeatureTuple(spark: SparkSession, patients: RDD[(String, Date)],
                                     labs: Dataset[LabResult]): RDD[FeatureTuple] = {

    val scPatientMap = spark.sparkContext.broadcast(patients.collectAsMap)

    labs.rdd
      .filter(lab => {
        val patient = scPatientMap.value.find(x => x._1 == lab.patientID).get
        patient._2.after(lab.date) && (patient._2.getTime - lab.date.getTime) <= obervationWindow
      })
      .map(x => ((x.patientID, x.testName), (x.value, 1.0)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1/x._2._2))
  }

  def constructFeatureVector(sc: SparkContext, patients: RDD[Patient], allFeature: RDD[FeatureTuple],
                             featurePath: String): Unit = {

    val featureMap = allFeature
      .map(_._1._2)
      .distinct
      .sortBy(x => x)
      .zipWithIndex
      .collectAsMap()

    val numFeature = featureMap.size
    println(s"Total Feature count: ${numFeature}")

    val patientMap = patients.map(x => (x.patientID, x.isDead)).collectAsMap()

    val scFeatureMap = sc.broadcast(featureMap)
    val scPatientMap = sc.broadcast(patientMap)

    val result = allFeature
      .map{case((patient, patientFeature), value) => (patient, (scFeatureMap.value(patientFeature), value))}
      .groupByKey
      .map { case (patient, indexedFeatures) =>

        val label = scPatientMap.value(patient)
        val featureVector = Vectors.sparse(numFeature, indexedFeatures.toList.map(x => (x._1.toInt, x._2)))
        LabeledPoint(label, featureVector)
      }
    MLUtils.saveAsLibSVMFile(result, featurePath)
    println(s"Result count: ${result.count}")
  }
}
