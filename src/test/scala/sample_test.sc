import org.json4s.jackson.JsonMethods

import scala.io.Source

object test{
  val test = Source.fromFile("/Users/sachio/IdeaProjects/kl_trip_log/src/test/resource/conf_test.json").getLines().mkString("")
  var json_map = JsonMethods.parse(test)
  implicit val formats = org.json4s.DefaultFormats
  json_map.extract[Map[String,Map[String,String]]].get("test_transform.jar")

}