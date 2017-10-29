import SparkJobTemplate.SparkJobConf
object SparkJob_DBReadJson {
   def main(args:Array[String]): Unit ={
     var test = new SparkJobConf("Kl_transform.jar",args(0),"test2")
     print(test.db_input)
     assert(test.db_input == "ckpj_etl.testin")
   }

}
