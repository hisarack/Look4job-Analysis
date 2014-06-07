
//REFERENCE : https://github.com/spray/spray-json
import spray.json._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import java.io.Serializable

import com.mongodb.casbah.Imports._

import JobListProtocol._

class ComEntry(val _mean: Double, val _vari: Double, val _count: Int) extends Serializable {
   var mean: Double = _mean
   var vari: Double = _vari
   var count: Int = _count
}

/* Report-formated class */

class RJob() {
   var ADDRESS: String = null
   var APPEAR_DATE: String = null
   var DESCRIPTION: String = null
   var J: String = null
   var JOB: String = null
   var JOB_ADDRESS: String = null
   var LANGUAGE1: String = null
   var LANGUAGE2: String = null
   var LANGUAGE3: String = null
   var STARTBY: String = null
}

class RCompany() {
   var C: String = null
   var NAME: String = null
   var LINK: String = null
   var PROFILE: String = null
   var WELFARE: String = null
   var PRODUCT: String = null
   var jobs: List[RJob] = null

   var ACTIVATE: Int = 0
}


object JobAnalysis {
   def date2timestamp(date: String) : Int = {
      val yyyy = date.substring(0,4).toInt
      val mm = date.substring(4,6).toInt
      val dd = date.substring(6,8).toInt

      return (yyyy-1900)*365+mm*30+dd
   }
   
   def getDiffDay(timestamp: Int) : Int = {
      val format = new java.text.SimpleDateFormat("yyyyMMdd")
      val curTimestamp = date2timestamp(format.format(new java.util.Date()))
      return curTimestamp-timestamp
   }

   def createJobClusterGroupByCompany(rdd: RDD[String], jC: HashMap[String, ComEntry]) {
      
      val jLRDD = rdd.map(line => (
         line.parseJson.convertTo[Job].C,
         date2timestamp(line.parseJson.convertTo[Job].APPEAR_DATE))
      ).groupByKey()

      jLRDD.collect.foreach(println)
      
      jLRDD.map{ case (company, jobList) =>
         var varTotal:Int = 0
         var total:Int = 0
         var count:Int = 0
         for(job <- jobList){
            total = total + job.toInt
            count = count + 1
         }
         val mean:Double = (total).toDouble / (count).toDouble
         for(job <- jobList){
            varTotal = varTotal + ((job.toInt - mean).abs).toInt
         }
         val vari:Double = (varTotal).toDouble / (count).toDouble
         (company,mean,vari,count)       
      }
      .collect()
      .foreach{ case(company,mean,vari,count) =>
            jC.put(company,new ComEntry(mean,vari,count))
      }  
   }
   
   def createReport(rdd: RDD[String], jC: HashMap[String, ComEntry]) {
      
      var rcompanyList =List[RCompany]()

      val cRDD = rdd.map{ case(line) =>
         val j:Job = line.parseJson.convertTo[Job]
         if(!jC.contains(j.C))(null,null)
         else (j.C,j)
      }.groupByKey()
 

      val mongo = MongoClient("localhost",27017)
      val db = mongo("jobFinder")
      val col = db("jc")
      var cObj:MongoDBObject = null
      var jBuilder:com.mongodb.casbah.commons.MongoDBListBuilder = null
      var jObj:MongoDBObject = null

      cRDD.collect().foreach{ case(company,jobList) =>
      
         jBuilder = MongoDBList.newBuilder

         if(company == null){return}

         var rcompany:RCompany = new RCompany()
         var rJobTmp:RJob = null
         var rComEntry:ComEntry = null

         for(job <- jobList){
            if(rcompany.C == null){
               rComEntry = jC.getOrElse(job.C,null)
               rcompany.C = job.C
               rcompany.NAME = job.NAME
               rcompany.ACTIVATE = getDiffDay((rComEntry.mean).toInt)
               rcompany.PROFILE = job.PROFILE
               rcompany.WELFARE = job.WELFARE
               rcompany.PRODUCT = job.PRODUCT
               rcompany.jobs = List[RJob]()
               
               cObj = MongoDBObject(
                        "index"   -> job.C,
                        "name"    -> job.NAME,
                        "profile" -> job.PROFILE,
                        "welfare" -> job.WELFARE,
                        "product" -> job.PRODUCT,
                        "activate" -> getDiffDay((rComEntry.mean).toInt)
                     ) 
            }
            rJobTmp = new RJob()
            rJobTmp.ADDRESS = job.ADDRESS
            rJobTmp.APPEAR_DATE = job.APPEAR_DATE
            rJobTmp.DESCRIPTION = job.DESCRIPTION
            rJobTmp.J = job.J
            rJobTmp.JOB = job.JOB
            rJobTmp.JOB_ADDRESS = job.JOB_ADDRESS
            rJobTmp.LANGUAGE1 = job.LANGUAGE1
            rJobTmp.LANGUAGE2 = job.LANGUAGE2
            rJobTmp.LANGUAGE3 = job.LANGUAGE3
            //rJobTmp.SAL_MONTH_HEIGHT = job.SAL_MONTH_HEIGHT
            //rJobTmp.SAL_MONTH_LOW = job.SAL_MONTH_LOW
            rJobTmp.STARTBY = job.STARTBY
            rcompany.jobs = rcompany.jobs :+ rJobTmp

            jObj = MongoDBObject(
               "address"      -> job.ADDRESS,
               "appear_date"  -> job.APPEAR_DATE,
               "description"  -> job.DESCRIPTION,
               "j"            -> job.J,
               "job"          -> job.JOB,
               "job_address"  -> job.JOB_ADDRESS,
               "language1"    -> job.LANGUAGE1,
               "language2"    -> job.LANGUAGE2,
               "language3"    -> job.LANGUAGE3,
               "startby"      -> job.STARTBY
            )
            jBuilder += jObj
         }
         rcompanyList = rcompanyList :+ rcompany

         cObj = cObj ++ MongoDBObject("jobs" -> jBuilder.result)
         col.insert(cObj)
      }

   }

   def dropJobCollection() {
   
      val mongo = MongoClient("localhost",27017)
      val db = mongo("jobFinder")
      val col = db("jc")
   
      col.remove(MongoDBObject.empty)
   }

   def main(args: Array[String]) {
      
      var logFile:String = null
      var logData:RDD[String] = null
      var jC = new HashMap[String,ComEntry]

      val sc = new SparkContext("local", "Simple App", "/root/spark-0.9.1/",
         List("target/scala-2.10/jobfinder_2.10-1.0.jar"))
      
      dropJobCollection()

      logFile = args(0)
      logData = sc.textFile(logFile, 2).cache()
      createJobClusterGroupByCompany(logData,jC)
      createReport(logData,jC)
 
   }
}
