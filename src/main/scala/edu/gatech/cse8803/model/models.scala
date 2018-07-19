/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.util.Date

case class Diagnostic(patientID:String, ICD9: String)

case class LabResult(patientID: String, eventID: String, date: Date, value: Double)

case class Events(patientID: String, eventID: String, date: Date, value: Double)

case class Medication(patientID: String, date: Date, medicine: String)
