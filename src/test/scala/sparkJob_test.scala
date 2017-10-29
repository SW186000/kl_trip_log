import SparkJobTemplate.SparkJobConf
import org.apache.spark.sql.SparkSession

object sparkJob_test {
  def main(args:Array[String]) = {
    var test = new SparkJobConf("Kl_transform.jar",args(0),args(1))
    var spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()

    test.setSparkInfo(spark)
    spark.conf.getAll.foreach{case (key,value) => printf(key + "=" + value + "¥r¥n")}

  }
}
