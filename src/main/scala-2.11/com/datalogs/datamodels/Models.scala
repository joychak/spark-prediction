package com.datalogs.datamodels

import java.sql.Date

/**
  * Created by joychak on 1/7/17.
  */

case class Patient(patientID: String, isDead: Double, dod: Date)

case class PatientEvent(patientID: String, eventID: String, eventDesc: String, timestamp: Date, value: Double)

case class Diagnostic(patientID:String, date: Date, code: String)

case class LabResult(patientID: String, date: Date, testName: String, value: Double)

case class Medication(patientID: String, date: Date, medicine: String)
