package com.datalogs.utils

import java.sql.Date
import java.text.SimpleDateFormat

import com.datalogs.datamodels._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by joychak on 1/7/17.
  */
object DataLoad {

  def loadPatientData(spark: SparkSession, inputPath: String)
  : (Dataset[Patient], Dataset[Diagnostic], Dataset[Medication], Dataset[LabResult], Dataset[PatientEvent]) = {

    import spark.implicits._

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val (deadPatientData, rawEventData) = loadRawData(spark, inputPath, dateFormat)

    val diagData = rawEventData
      .filter(_.eventID.toUpperCase.startsWith("DIAG"))
      .map(event => Diagnostic(event.patientID, event.timestamp, event.eventID))
    println(s"${diagData.count} diagnosis data loaded ...")

    val medData = rawEventData
      .filter(_.eventID.toUpperCase.startsWith("DRUG"))
      .map(event => Medication(event.patientID, event.timestamp, event.eventID))
    println(s"${medData.count} medication data loaded ...")

    val labData = rawEventData
      .filter(_.eventID.toUpperCase.startsWith("LAB"))
      .map(event => LabResult(event.patientID, event.timestamp, event.eventID, event.value))
    println(s"${labData.count} lab data loaded ...")

    val scDeadPatientMap = spark.sparkContext.broadcast(deadPatientData.map(_.patientID).collectAsList)
    println(s"${deadPatientData.count} dead patient data loaded ...")

    val alivePatient = rawEventData
      .filter(event => !scDeadPatientMap.value.contains(event.patientID))
      .map(_.patientID)
      .distinct
      .map(patient => new Patient(patient, 0.0, null))

    val allPatient = deadPatientData.union(alivePatient)
    println(s"${allPatient.count} patient data loaded ...")

    (allPatient, diagData, medData, labData, rawEventData)
  }


  def loadRawData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat)
  : (Dataset[Patient], Dataset[PatientEvent]) = {
    (loadRawMortalityData(spark, inputPath, dateFormat),
      loadRawEventData(spark, inputPath, dateFormat))
  }

  def loadRawMortalityData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[Patient] = {

    import spark.implicits._

    List(inputPath + "/mortality_events.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "PATIENTS"))

    spark.sqlContext.sql(
      """
        |SELECT patient_id,timestamp,label
        |FROM PATIENTS
      """.stripMargin)
      .map(r => Patient(
        r(0).toString,
        if (r(2).toString.toLowerCase=="1") 1.0 else 0.0,
        new Date(dateFormat.parse(r(1).toString).getTime)
      )
    )
  }

  def loadRawEventData(spark: SparkSession, inputPath: String, dateFormat: SimpleDateFormat): Dataset[PatientEvent] = {

    import spark.implicits._

    List(inputPath + "/events.csv")
      .foreach(CSVUtils.loadAnyCSVAsTable(spark, _, "EVENTS"))

    spark.sqlContext.sql(
      """
        |SELECT patient_id,event_id,event_description,timestamp,value
        |FROM EVENTS
      """.stripMargin)
      .map(r => PatientEvent(
        r(0).toString,
        r(1).toString,
        r(2).toString,
        new Date(dateFormat.parse(r(3).toString).getTime),
        if (!r(4).toString.isEmpty) r(4).toString.toDouble else 0.0
      )
    )
  }
}
