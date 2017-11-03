import org.json4s.jackson.JsonMethods

import scala.io.Source

object test{
  val test = Source.fromFile("/Users/sachio/IdeaProjects/kl_trip_log/src/test/resource/conf_test.json").getLines().mkString("")
  var json_map = JsonMethods.parse(test)
  implicit val formats = org.json4s.DefaultFormats
  json_map.extract[Map[String,Map[String,String]]].get("test_transform.jar")

  case class Person(name:String, class_no:Int)
  case class Num_person(name:String, class_no:Int, order:Int)
  val rec_test = List(List(1,2),List(2,2),List(3,3),List(4,4))

  def row_test(row1:List[Int], row2:List[List[Int]]): List[List[Int]]= {
    print(row2)
    row2 match {
          case Nil => Nil
          case row1 :: xs => List(row1(0), row1(1), xs.head(0)) :: row_test(xs.head, xs)
          case _  => List(List(row2(0)(0), row2(0)(1), 0))
        }
  }

  rec_test.foldRight[List[List[Int]]](List[List[Int]]())({
    (x,y) =>
      if (y.isEmpty) List(x(0),x(1),0) :: y
      else List(x(0),x(1),y.head(1)) :: y
  })


  val person_test = List(Person("jon",1),Person("peter",2),Person("bes",3))

  person_test.foldLeft[List[Num_person]](List[Num_person]())({
    (acc_person,pe) =>
      if (acc_person.isEmpty) Num_person(pe.name,pe.class_no,0) :: acc_person
      else Num_person(pe.name,pe.class_no,acc_person.head.class_no) :: acc_person
  })


  row_test(Nil,rec_test)

}