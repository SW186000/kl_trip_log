package Kl_trip_transform

import SparkJobTemplate.SparkJobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kl_trip_transform {


  def main(args: Array[String]): Unit = {
    val spark_job = new SparkJobConf("Kl_trip_transform",args(0),args(1))
    var spark = SparkSession.builder().getOrCreate()
    spark_job.setSparkInfo(spark)

    /* RDD input_data = spark_job.LoadInputRddData()
    RDD intermediate_data = input_data.aggregatebyKey().map(add_type).filter().toDF()
    spark_job.OutputToHive(intermediate_data) */

  }

/*  def add_type(): DataFrame = {

  } */




}
