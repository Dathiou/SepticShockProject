
package edu.gatech.cse8803.features

import java.util.{Calendar, Date}

import edu.gatech.cse8803.model.{Events, LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.Try
import scala.util.control.Breaks._


object FeatureConstruction {

  type FeatureTuple = ((String, String), Double)
  type FeatureTuple1 = (String, (Int, Double))


  def findPos (sc: SparkContext, diagnostic : RDD[Diagnostic],charEvents : RDD[Events],lab : RDD[LabResult]): RDD[(String,Int)]={

    val patientSepticShock = diagnostic.filter { a => a.ICD9 == "78552"  }.map(a => (a.patientID, 1)).distinct

    val SBP = Set("455","6","51","6701","220179","220050" )

    val patientSevereSepsis = diagnostic.filter { a => (a.ICD9 == "99592") }.map(_.patientID)

val severeHypo =  patientSevereSepsis.distinct().collect().toSet
    val severeWith = charEvents.filter(a => severeHypo.contains(a.patientID))

    val SBPrdd = severeWith.filter(a => SBP.contains(a.eventID)).map(a => (a.patientID,("SBP",a.value,a.date.getTime())))
    //detects drop of SBP below 90 for 30 min
    val SBPselecdate1= SBPrdd.groupByKey().map{case (a,b) =>
        var ref = 0.0
        var diffind = 0
        var res = 0
breakable {

  for (s <- b.toList.sortBy(_._3)) {
    if (diffind == 0) { //if first
    if (s._2 < 90) {
      ref = s._3
      val diffind = 1
    }
  } else {
        if(s._2 > 90){ diffind = 0}
        else{
          if (s._3-ref>30*60*1000){ //30 min
            diffind = 0
            res = 1 //positif
            break
          }
        }
      }
}
}
        (a,res)
      }

    val septicShock = SBPselecdate1.filter(a => a._2==1)

    val posPatientRDD = sc.union(septicShock,patientSepticShock).distinct()

    posPatientRDD
  }

  def IndDateNeg(sc: SparkContext,charEvents : RDD[Events],Lab : RDD[LabResult],negSet : Set[String]): (RDD[(String,Long)]) = {

    sc.union(Lab.filter(a => negSet.contains(a.patientID)).map(a => (a.patientID, a.date)), charEvents.filter(a => negSet.contains(a.patientID)).map(a => (a.patientID, a.date))).reduceByKey((a, b) => if (a.getTime >= b.getTime) a else b).map(a => (a._1,a._2.getTime))


  }
  def IndDatePos(sc: SparkContext,charEvents : RDD[Events],Lab : RDD[LabResult],posSet : Set[String]): (RDD[(String,Long)]) = {
    val events=charEvents.filter(a => posSet.contains(a.patientID))
    val indDate = events.filter( a => a.eventID=="SBP" && a.value<90).map(a => (a.patientID,a.date)).reduceByKey((a, b) => if (a.getTime <= b.getTime) a else b)
    val indDateLast = sc.union(events.map(a => (a.patientID,a.date)),Lab.filter(a => posSet.contains(a.patientID)).map(a => (a.patientID,a.date))).reduceByKey((a, b) => if (a.getTime >= b.getTime) a else b)

    val indxdate = sc.union(indDate,indDateLast).reduceByKey((a, b) => if (a.getTime <= b.getTime) a else b).map( a => (a._1,hourChangedOpt(a._2.getTime,-22)))

    (indxdate)
  }




  def aggregate1(sc: SparkContext,diagnostic : RDD[Diagnostic],charEvents : RDD[FeatureTuple],lab : RDD[FeatureTuple]): (RDD[FeatureTuple]) = {
    val aggdiag = diagnostic.map(a => ((a.patientID, a.ICD9), a)).groupBy(_._1).mapValues(_.map(_._2).size.toDouble).map(a => ((a._1._1, a._1._2), a._2)) // count diagnostics
    val agglab = lab.groupBy(_._1).map { case (a, b) => (a, b.map(_._2).sum / b.size) }.map(a => ((a._1._1, a._1._2), a._2)) //avg lab results
    val aggevents = charEvents.groupBy(_._1).map { case (a, b) => (a, b.map(_._2).sum / b.size) }.map(a => ((a._1._1, a._1._2), a._2)) //avg chart events
    sc.union(aggdiag,aggevents,agglab)
  }



  def selectDiag(diagnostic : RDD[Diagnostic]): (RDD[Diagnostic])={
    diagnostic.filter{a =>
      // if (Try(a.ICD9.take(3)).isSuccess ){
      (a.ICD9=="99590"|| a.ICD9=="99591"|| a.ICD9=="99592"|| a.ICD9=="99593"|| a.ICD9=="99594"|| a.ICD9=="78552" || a.ICD9.take(3)=="571" || a.ICD9=="V5865" || a.ICD9=="V580" || a.ICD9=="V581" || a.ICD9.take(3)=="042" || a.ICD9=="2080" || a.ICD9.take(3)=="202" || a.ICD9.take(3)=="200" || a.ICD9.take(3)=="201" || a.ICD9.take(3)=="203" || a.ICD9.take(3)=="204" || a.ICD9.take(3)=="205" || a.ICD9.take(3)=="206" || a.ICD9.take(3)=="207" || a.ICD9.take(3)=="208" || a.ICD9.take(3)=="428" || a.ICD9.take(3)=="571"|| a.ICD9=="5856"|| a.ICD9=="42822"|| a.ICD9=="42842"|| a.ICD9=="51883"|| a.ICD9.take(3)=="250"|| (if(Try(a.ICD9.take(3).toDouble).isSuccess) {a.ICD9.take(3).toInt<=165 && a.ICD9.take(3).toInt>=140|| a.ICD9.take(3).toInt<=175 && a.ICD9.take(3).toInt>=170|| a.ICD9.take(3).toInt<=199 && a.ICD9.take(3).toInt>=179 } else {false}) )
      //  } else {false}
    }

  }

def groupDiag(diagnostic : RDD[Diagnostic]): (RDD[Diagnostic])={
  diagnostic.map{a =>
  // if (Try(a.ICD9.take(3)).isSuccess ){
  var  r= "0"
  if  (a.ICD9.take(3)=="571") { r = "LiverOrCyrrhosis"}
  else if ( a.ICD9=="V5865" || a.ICD9=="V580" || a.ICD9=="V581" || a.ICD9.take(3)=="042" || a.ICD9=="2080" || a.ICD9.take(3)=="202"){
  r="ImmunoCompromised"}

  else if (a.ICD9.take(3)=="200" || a.ICD9.take(3)=="201" || a.ICD9.take(3)=="203" || a.ICD9.take(3)=="204" || a.ICD9.take(3)=="205" || a.ICD9.take(3)=="206" || a.ICD9.take(3)=="207" || a.ICD9.take(3)=="208")
{ r="Hemtologicalmalignancy"}
  else if (a.ICD9.take(3)=="428" ){r="ChroHF" }
  else if (a.ICD9.take(3)=="571"|| a.ICD9=="5856"|| a.ICD9=="42822"|| a.ICD9=="42842"|| a.ICD9=="51883") {r="ChroOrganInsuf" }
  else if (a.ICD9.take(3)=="250" ){ r="Diabetes" }
  else if (a.ICD9=="99590"|| a.ICD9=="99591"|| a.ICD9=="99592"|| a.ICD9=="99593"|| a.ICD9=="99594"|| a.ICD9=="78552"){r = a.ICD9}
  else if (Try(a.ICD9.take(3).toDouble).isSuccess) {
    if (a.ICD9.take(3).toInt<=165 && a.ICD9.take(3).toInt>=140|| a.ICD9.take(3).toInt<=175 && a.ICD9.take(3).toInt>=170|| a.ICD9.take(3).toInt<=199 && a.ICD9.take(3).toInt>=179 )
      {r="Metastica carcinoma" }
}

  else {
  r="0"
}

  new Diagnostic(a.patientID,r)

}.filter(a=> a.ICD9!="0")

}

  def groupEvents(events : RDD[Events]): (RDD[Events])={

    val UO = Set("55","56","57","61","65","69","85","94","96","288","405","428","473","651","715","1922","2042","2068","2111","2119","2130","2366","2463","2507","2510","2592","2676","2810","2859","3053","3175","3462","3519","3966","3987","4132","4253","5927")

    events.map{a =>

      var r = "0"
      if  (a.eventID=="6"|| a.eventID=="51" || a.eventID=="455" || a.eventID=="6701" || a.eventID=="220179" || a.eventID=="220050") {r="SBP"}
      else if ( a.eventID=="219" || a.eventID=="615" || a.eventID=="618"){r="RR"}
      else if ( a.eventID=="211" || a.eventID=="220045"){r="HR"}
      else if ( UO.contains(a.eventID)){ var r="UO"}
      else if ( a.eventID=="51300" || a.eventID=="51301"){r="WBC"}
      else if ( a.eventID=="50822" || a.eventID=="50971"){ r="Potassium"}
              else {
        r="0"
      }

      new Events(a.patientID,r,a.date,a.value)

    }.filter(a=> a.eventID!="0")

  }

  def groupLab(lab : RDD[LabResult]): (RDD[LabResult])={
    lab.map{a =>

      var r = "0"
      if  (a.eventID=="50803"|| a.eventID=="50804" || a.eventID=="50882") { r="Bicarbonate"}
      else if ( a.eventID=="51006"){  r="BUN"}
      else if ( a.eventID=="50912"){  r="CREATININE"} // to be completed
      else {
         r="0"
      }

      new LabResult(a.patientID,r,a.date,a.value)

    }.filter(a=> a.eventID!="0")

  }




  def replaceFeatureID1(sc: SparkContext, Agg : RDD[FeatureTuple]): (RDD[FeatureTuple1],Int,RDD[(String,Int)]) = {

    val id_events = Agg.groupBy(_._1._2).zipWithIndex.map(a => (a._1._1, a._2.toInt))

    val events_transformed = Agg.map(a => (a._1._2, a)).join(id_events).map(a => (a._2._1._1._1, (a._2._2, a._2._1._2)))
    val MaxInd = id_events.map(_._2).count

    val feat = events_transformed
    (feat,MaxInd.toInt,id_events)

  }


  def filterLabByDate(filteredLab : RDD[LabResult]): RDD[FeatureTuple] = {
    val filteredLabDate = filteredLab.map(a => ((a.patientID), (a.eventID, a.value, a.date))).map(a => ((a._1, a._2._1), a._2._2))
    filteredLabDate
  }

    def filterEventsByDate(filteredEvents : RDD[Events],filteredLab : RDD[LabResult], indexDate : RDD[(String,Long)]): RDD[FeatureTuple] = {
    val filteredEventsDate = filteredEvents.map(a => ((a.patientID), (a.eventID, a.value, a.date))).join(indexDate).filter(a => a._2._1._3.getTime <= a._2._2 && a._2._1._3.getTime >= hourChangedOpt(a._2._2, -30))
      .map(a => ((a._1, a._2._1._1), a._2._1._2))

    filteredEventsDate
  }


  def hourChangedOpt(date : Long, hourToAdd: Int):Long = {
    date + hourToAdd*3600*1000
  }

    def constructF(sc: SparkContext, feature: RDD[FeatureTuple1], MaxInd : Int): RDD[(String, Vector)] = {
    feature.cache()
    val features_final = feature.groupByKey()
    val f2 = features_final.map { case (tar, feat) =>
      val featvec = Vectors.sparse(MaxInd, feat.toSeq)
      (tar, featvec)
    }
      f2.take(2).foreach(println)
    f2
  }

}


