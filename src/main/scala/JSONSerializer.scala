
//REFERENCE : https://github.com/spray/spray-json
import spray.json._
//import DefaultJsonProtocol._ // !!! IMPORTANT, else `convertTo` and `toJson` won't work correctly

/* Input-formated class */

case class Job(
   ADDRESS: String,
   APPEAR_DATE: String,
   C: String,
   DESCRIPTION: String,
   J: String,
   JOB: String,
   JOB_ADDRESS: String,
   LANGUAGE1: String,
   LANGUAGE2: String,
   LANGUAGE3: String,
   LINK: String,
   NAME: String,
   PRODUCT: String,
   SAL_MONTH_HIGHT: Option[String] = None,
   SAL_MONTH_LOW: Option[String] = None,
   STARTBY: String,
   PROFILE: String,
   WELFARE: String
)

case class JobList[T](
   RECORDCOUNT: String,
   PAGECOUNT: String,
   PAGE: String,
   TOTALPAGE: String,
   data: List[T]
)

object JobListProtocol extends DefaultJsonProtocol {
   implicit def jobListFormat[T :JsonFormat] = jsonFormat5(JobList.apply[T])
   implicit val jobFormat = jsonFormat18(Job)
}
 
import JobListProtocol._

object JSONSerializer {
   def removeSpecialCharacter(str: String) : String = {
      return str.filter(_ >= ' ').replaceAll("\""," ").replaceAll("\\\\"," ")
   }
   def jobEntrySerialize(jobIndex: Int, capture_date: String, j: Job) : String = {
      var output_str = ""
      var tmp_str = ""
   
      output_str = output_str.concat("{\"index\":\""+jobIndex+"\",")
      tmp_str = removeSpecialCharacter(capture_date)
      output_str = output_str.concat("\"capture_date\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.ADDRESS)
      output_str = output_str.concat("\"ADDRESS\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.NAME)
      output_str = output_str.concat("\"NAME\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.APPEAR_DATE)
      output_str = output_str.concat("\"APPEAR_DATE\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.C)
      output_str = output_str.concat("\"C\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.DESCRIPTION)
      output_str = output_str.concat("\"DESCRIPTION\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.J)
      output_str = output_str.concat("\"J\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.JOB)
      output_str = output_str.concat("\"JOB\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.JOB_ADDRESS)
      output_str = output_str.concat("\"JOB_ADDRESS\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.LANGUAGE1)
      output_str = output_str.concat("\"LANGUAGE1\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.LANGUAGE2)
      output_str = output_str.concat("\"LANGUAGE2\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.LANGUAGE3)
      output_str = output_str.concat("\"LANGUAGE3\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.LINK)
      output_str = output_str.concat("\"LINK\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.PRODUCT)
      output_str = output_str.concat("\"PRODUCT\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.STARTBY)
      output_str = output_str.concat("\"STARTBY\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.PROFILE)
      output_str = output_str.concat("\"PROFILE\":\""+tmp_str+"\",")
      tmp_str = removeSpecialCharacter(j.WELFARE)
      output_str = output_str.concat("\"WELFARE\":\""+tmp_str+"\"}\n")

      //println(index," ",j.ADDRESS)

      return output_str
   }
   def main(args: Array[String]) {
      //var json = Job("address","apear_date","c","description","j","job","job_address","language1","language2","language3","link","product","salh","sall","","","").toJson
      //var job = json.convertTo[Job]
      
      val input = scala.io.Source.fromFile(args(0))
      val output = new java.io.FileWriter(new java.io.File(args(1)),true)
      val lines = input.mkString

      val jsonAst = lines.parseJson
      val jobList = jsonAst.convertTo[JobList[Job]]
      var rindex = 0
      var jobIndex = args(2).toInt
      var capture_date = args(3)
      for(rindex <- 0 until jobList.PAGECOUNT.toInt){
           //println("job ",rindex," : ",jobList.data(rindex).ADDRESS,jobList.data(rindex).DESCRIPTION)
           output.write(jobEntrySerialize(jobIndex+rindex,capture_date,jobList.data(rindex)))      
      }
      input.close()
      output.close()
   }
}
