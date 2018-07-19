
package edu.gatech.cse8803.main

import java.io.File
import java.text.SimpleDateFormat
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.ioutils.PCA
import edu.gatech.cse8803.model.{Events, Diagnostic, LabResult}

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{SchemaRDD, Row, SQLContext}
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint


object Main {


  def main(args: Array[String]) {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level


    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext

    val sqlContext = new SQLContext(sc)


    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  val (candidateICD,candidateLabICD) = loadFeaturesShortList(sc) //features code
//  val (diagnosti, la, charEvent) = loadRddRawData(sc,sqlContext)
//
//load tables. (these files are the original rdd filtered to keep only the indormation needed
    val chart = sc.textFile("InitialData/CHARTEVENTSSingleFiltered.csv")
    val charttring = chart.map(l => l.split(",")).filter(a => candidateICD.contains(a(1)))   //filter the features needed
    val filteredEvents1 : RDD[Events] = charttring.map(r => new Events(r(0),r(1),dateFormat.parse(r(2)),r(3).toDouble))

    val diag = sc.textFile("InitialData/DIAGSingleFiltered.csv")
    val diagtring = diag.map(l => l.split(","))
    val filteredDiag1 : RDD[Diagnostic] = diagtring.map(r => new Diagnostic(r(0),r(1)))

    val labSQL = sc.textFile("InitialData/LABSingleFiltered.csv")
    val labtring = labSQL.map(l => l.split(",")).filter(a => candidateLabICD.contains(a(1))) //filter the features needed
    val filteredLab1 : RDD[LabResult] = labtring.map(r => new LabResult(r(0),r(1),dateFormat.parse(r(2)),r(3).toDouble))

//group the features per category
    val filteredLab = FeatureConstruction.groupLab(filteredLab1)
    val filteredEvents = FeatureConstruction.groupEvents(filteredEvents1)
    val filteredDiag = FeatureConstruction.groupDiag(FeatureConstruction.selectDiag(filteredDiag1))


   val AllPat = sc.union(filteredDiag.map(_.patientID), filteredLab.map(_.patientID),filteredEvents.map(_.patientID)).distinct()
    //select sepsis related patients
    val posPatientRDD = FeatureConstruction.findPos (sc, filteredDiag,filteredEvents,filteredLab)
    //val posPatient = sc.broadcast(posPatientRDD.keys.collect().toSet)
    val posPatient = sc.broadcast(posPatientRDD.keys.collect().toSet)
    val nPos =  posPatient.value
    //val negPatientRDD = AllPat.filter { a => !posPatient.contains(a) } //filter severe septis and septic shock
    val negPatientRDD = AllPat.subtract(posPatientRDD.map(_._1))
    val negPatient = sc.broadcast(negPatientRDD.collect().toSet)
    val nNeg =  negPatient.value
    val patWithlabel = sc.union(posPatientRDD, negPatientRDD.map(a => (a, 0)))
    val patWithlabel1 = patWithlabel

    //create index date
    val posIndexdate =FeatureConstruction.IndDatePos(sc,filteredEvents,filteredLab,posPatient.value)
    val negIndexdate = FeatureConstruction.IndDateNeg(sc,filteredEvents,filteredLab,negPatient.value)
    val indexDate = sc.union(posIndexdate, negIndexdate) // max date for each patient

   val filteredLabDate = FeatureConstruction.filterLabByDate(filteredLab)
    val filteredEventsDate = FeatureConstruction.filterEventsByDate(filteredEvents,filteredLab, indexDate)

    //aggregate features
    val AllFeatures = FeatureConstruction.aggregate1(sc,filteredDiag,filteredEventsDate,filteredLabDate)

//replace feature name by feature ID
    val (feat,indMax,indFeat) = FeatureConstruction.replaceFeatureID1(sc,AllFeatures)

    //construct parse vector for each patient
val rawFeatures = FeatureConstruction.constructF(sc, feat, indMax)

    val i2 = modeling(patWithlabel, rawFeatures)
  }

//to save rdd to csv
  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

    def modeling(Label: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): Double = {

      /** scale features */
      val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
      val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray))) })
      val featuresLabeled = features.join(Label).map ( a => (LabeledPoint(a._2._2,a._2._1),a._1))


     val pca = new PCA(10).fit(featuresLabeled.map(_._1).map(_.features))

      // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
    val projected = featuresLabeled.map{case (p,a) => (p.copy(features = pca.transform(p.features)),a)}
//val projected =featuresLabeled



      var splits = projected.randomSplit(Array(0.6, 0.4))
      var training = splits(0).cache()
      var test = splits(1)

         val model = SVMWithSGD.train(training.map(_._1), 100,2,0.1)

      model.clearThreshold()


      var predictionAndLabels = test.map { case (LabeledPoint(label, features), a) =>
        val prediction = model.predict(features)
        (prediction, label, a)
      }
      val metrics = new BinaryClassificationMetrics(predictionAndLabels.map(a => (a._1, a._2)))
      val auROC = metrics.areaUnderROC
      println("Area under ROC = " + auROC)
      metrics.areaUnderROC

    }



  def loadFeaturesShortList(sc: SparkContext): (Set[String],Set[String]) = {
    //val candidateFeatures = Source.fromFile("s3://cse8803-proj/mimic/features.csv").getLines().map(_.toLowerCase).toSet[String]
    val candidateFeatures = sc.textFile("mimic/features2.csv").collect().toSet[String]
    val candidateLabFeatures = sc.textFile("mimic/labfeatures2.csv").collect().toSet[String]
    (candidateFeatures,candidateLabFeatures)
  }


  def loadRddRawData(sc: SparkContext, sqlContext: SQLContext): (RDD[Diagnostic],RDD[LabResult],RDD[Events]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //s3://cse8803-proj/mimic/CHARTEVENTS_DATA_TABLE.csv
    val x  = CSVUtils.loadCSVAsTable(sqlContext: SQLContext, "mimic/LABEVENTS_DATA_TABLE.csv": String , "LAB")
    val x1  = CSVUtils.loadCSVAsTable(sqlContext: SQLContext, "mimic/CHARTEVENTS_DATA_TABLE.csv": String , "CHART")
    val x2  = CSVUtils.loadCSVAsTable(sqlContext: SQLContext, "mimic/DIAGNOSES_ICD_DATA_TABLE.csv": String , "Diagnostics")

    //val myApacheLogs = sc.textFile("MIMIC/DIAGNOSES_ICD_DATA_TABLE.csv")
    //val parts = myApacheLogs.map(l => l.split(","))
   // val diag_rdd : RDD[Diagnostic] = parts.map(p => new Diagnostic(p(0), p(2)))

    val chart = sqlContext.sql("SELECT SUBJECT_ID as patientID, ITEMID as eventID, CHARTTIME as date, VALUENUM as value  FROM CHART WHERE VALUENUM!='' ")
    //val lab = sqlContext.sql("SELECT SUBJECT_ID as patientID, ITEMID as eventID, CHARTTIME as date, ITEMID as eventID, VALUENUM as value  FROM LAB WHERE VALUENUM!='' limit 10")

    val chartevents_rdd : RDD[Events] = chart.rdd.map { r: Row => new Events(r.getString(0),r.getString(1),dateFormat.parse(r.getString(2)),java.lang.Double.valueOf(r.getString(3)))}
   // chartevents_rdd.saveAsObjectFile("rdd/chartevents")

    val diag = sqlContext.sql("SELECT SUBJECT_ID as patientID,ICD9_CODE as eventID  FROM Diagnostics where ICD9_CODE!=''")
    val diag_rdd : RDD[Diagnostic] = diag.rdd.map { r: Row => new Diagnostic(r.getString(0),r.getString(1))}



    //diag_rdd.saveAsObjectFile("rdd/diag")





    val lab = sqlContext.sql("SELECT SUBJECT_ID as patientID, ITEMID as eventID, CHARTTIME as date, VALUENUM as value  FROM LAB WHERE VALUENUM!='' and CHARTTIME!='' and ITEMID!='' ")
    //val lab = sqlContext.sql("SELECT SUBJECT_ID as patientID, ITEMID as eventID, CHARTTIME as date, ITEMID as eventID, VALUENUM as value  FROM LAB WHERE VALUENUM!='' limit 10")

    val lab_rdd : RDD[LabResult] = lab.rdd.map {r: Row => new LabResult(r.getString(0),r.getString(1),dateFormat.parse(r.getString(2)),java.lang.Double.valueOf(r.getString(3)))}
    //lab_rdd.saveAsObjectFile("rdd/lab")
    (diag_rdd,lab_rdd,chartevents_rdd)
  }



  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")




}
