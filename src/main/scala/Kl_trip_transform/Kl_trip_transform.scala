package Kl_trip_transform

import SparkJobTemplate.SparkJobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

object Kl_trip_transform {
  case class KL_tran_src(mdn:String,loc_datetime:Double,loc_type:Int, ym:Int)

  def main(args: Array[String]): Unit = {
    val spark_job = new SparkJobConf("Kl_trip_transform.jar",args(0),args(1))
    var spark = SparkSession.builder().getOrCreate()
    spark_job.setSparkInfo(spark)

    val input_data = spark_job.LoadHiveTblToDS(spark)
    /* val intermediate_data = input_data.map(x => (x.toSeq._1,x)).aggregatebyKey().map(add_type).filter().toDF() */
    val intermediate_data = input_data.as[KL_tran_src].groupByKey({x => x.mdn}).mapGroups({case (x,y) => (x,add_type(y))})
    spark_job.ExportDataFrameToHive(spark,intermediate_data)

  }

  def add_type(x:Iterator[KL_tran_src]): DataFrame = {
    val data = x.toList.sortBy({case x => x.loc_datetime}).foldLeft(List[Any]())((x,y) => x match {
      case Nil => List(y.mdn,y.loc_datetime,y.loc_type,y.ym,1,None)
      case x::xs => xs match {
        case List[Any] => List(y.mdn,y.loc_datetime,y.loc_type,y.ym)
      }
  }
  }



}
