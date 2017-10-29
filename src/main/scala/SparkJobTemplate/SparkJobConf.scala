package SparkJobTemplate

import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

import scala.io.Source

case class DBObjInfoDetail(input:Map[String,String],output:Map[String,String])
case class SparkSessionJobConf(conf_map:Map[String,String])
case class DBObjInfo(input:String,output:String)

abstract class SparkJobConfBase(job_name:String, db_inf:String, conf_inf:String) {
  var db_input:String
  var db_output:String
  var conf:Map[String,String]
  /* var spark:SparkSession */

  implicit val formats = DefaultFormats

  def setConfInfo(job_name:String, conf_inf:String):Map[String,String] = {
    /* stub 
    Map(conf_inf -> "test1") */
    val json_source = Source.fromFile(conf_inf).getLines().mkString("")
    var json_map = JsonMethods.parse(json_source)
    json_map.extract[Map[String,Map[String,String]]].get(job_name).get
  }

  def setDBInfo(job_name:String, db_inf: String):DBObjInfo = {
    /* stub
    Map("input" -> "db.in_tbl","output" -> "db.out_tbl") */
    val json_source = Source.fromFile(db_inf).getLines().mkString("")
    var json_map = JsonMethods.parse(json_source)

    val TargetJobInOut = json_map.extract[Map[String,DBObjInfoDetail]].get(job_name)
    DBObjInfo(TargetJobInOut.map({x => x.input.get("db").getOrElse("") + "." + x.input.get("tbl").getOrElse("")}).getOrElse(""),
      TargetJobInOut.map({x => x.output.get("db").getOrElse("") + "." + x.output.get("tbl").getOrElse("")}).getOrElse(""))
  }


}

class SparkJobConf(job_name:String, db_inf:String,conf_inf:String) extends SparkJobConfBase(job_name:String, db_inf:String,conf_inf:String) {
  override var db_input: String = this.setDBInfo(job_name,db_inf).input
  override var db_output: String = this.setDBInfo(job_name,db_inf).output
  override var conf:Map[String,String] = this.setConfInfo(job_name,conf_inf)
  /* override var spark:SparkSession = this.setSparkInfo(conf_inf) */

  def setSparkInfo(spark: => SparkSession):Unit = {
    this.conf.foreach{case (key,value) => spark.conf.set(key,value)}
  }

}