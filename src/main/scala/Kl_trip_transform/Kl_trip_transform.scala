package Kl_trip_transform

import SparkJobTemplate.SparkJobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kl_trip_transform {


  def main(args: Array[String]): Unit = {
    val spark_job = new SparkJobConf("Kl_trip_transform",args(0),args(1))
    var spark = SparkSession.builder().getOrCreate()
    spark_job.setSparkInfo(spark)

    val input_data = spark_job.LoadHiveTblToRDD(spark)
    val intermediate_data = input_data.map(x => (x.toSeq._1,x)).aggregatebyKey().map(add_type).filter().toDF()
    spark_job.ExportDataFrameToHive(spark,intermediate_data)

  }

/*  def add_type(): DataFrame = {

  } */




}
