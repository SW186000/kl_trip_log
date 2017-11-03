package Kl_trip_transform

import SparkJobTemplate.SparkJobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.collection.parallel.mutable

object Kl_trip_transform {
  case class KL_tran_src(mdn:String,loc_datetime:Double,loc_type:Int, ym:Int)
  case class KL_tran_back_type(mdn:String,loc_datetime:Double,loc_type:Int,ym:Int,back_type:Int,back_dttm:Double)
  case class KL_tran_goto_type(mdn:String,loc_datetime:Double,loc_type:Int,ym:Int,back_type:Int,back_dttm:Double,goto_type:Int,goto_dttm:Double)

  def main(args: Array[String]): Unit = {
    val spark_job = new SparkJobConf("Kl_trip_transform.jar",args(0),args(1))
    /* var spark_tmp = SparkSession.builder().getOrCreate() */
    val spark = spark_job.setSparkInfo(SparkSession.builder().getOrCreate())
    import spark.implicits._
    implicit val KL_tran_srcEncoder = org.apache.spark.sql.Encoders.kryo[KL_tran_src]


    val input_data = spark_job.LoadHiveTblToDF(spark)
    /* val intermediate_data = input_data.map(x => (x.toSeq._1,x)).aggregatebyKey().map(add_type).filter().toDF() */
    val intermediate_data = input_data.as[KL_tran_src].groupByKey({x => x.mdn}).mapGroups({case (x,y) => (x,add_type(y))}).flatMap({case (x,y) => y}).toDF()
    spark_job.ExportDataFrameToHive(spark,intermediate_data)

  }

  def add_type(x:Iterator[KL_tran_src]): List[KL_tran_goto_type] = {
    val add_back_flg_data = get_back_type(Nil,x.toList.sortBy({case x => x.loc_datetime}))
    get_goto_type(Nil,add_back_flg_data.sortBy({case y => -y.loc_datetime}))
  }


  def get_back_type(prev_row:List[KL_tran_src], row_list_asc:List[KL_tran_src]): List[KL_tran_back_type] ={
    row_list_asc match {
      case Nil => Nil
      case x1 :: xs => KL_tran_back_type(x1.mdn, x1.loc_datetime, x1.loc_type, x1.ym, 0, 0) :: get_back_type(List(x1), xs)
      case prev_row :: x1 :: xs =>
        if (prev_row.loc_type == 100) KL_tran_back_type(x1.mdn, x1.loc_datetime,x1.loc_type, x1.ym, 10, prev_row.loc_datetime) :: get_back_type(List(x1),xs)
        else KL_tran_back_type(x1.mdn, x1.loc_datetime,x1.loc_type, x1.ym, 10, prev_row.loc_datetime) :: get_back_type(List(x1),xs)
    }

  }

  def get_goto_type(prev_row:List[KL_tran_back_type], row_list_asc:List[KL_tran_back_type]): List[KL_tran_goto_type] ={
    row_list_asc match {
      case Nil => Nil
      case x1 :: xs => KL_tran_goto_type(x1.mdn, x1.loc_datetime, x1.loc_type, x1.ym, 0, 0,0,0) :: get_goto_type(List(x1), xs)
      case prev_row :: x1 :: xs =>
        if (prev_row.loc_type == 10 & x1.back_type == 10) KL_tran_goto_type(x1.mdn, x1.loc_datetime,x1.loc_type, x1.ym, x1.back_type, x1.back_dttm, 10, prev_row.loc_datetime) :: get_goto_type(List(x1),xs)
        else KL_tran_goto_type(x1.mdn, x1.loc_datetime,x1.loc_type, x1.ym, x1.back_type, x1.back_dttm,10, prev_row.loc_datetime) :: get_goto_type(List(x1),xs)
    }

  }


}
