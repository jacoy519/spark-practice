import java.io.File
import java.util

import breeze.linalg.sum
import com.google.gson.Gson
import dt.config.RegularInvestSimulationConf
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

object RegularInvestBenefitSimulation {


  def main(args: Array[String]): Unit = {

    val str = "E:/Spark-practice/sparktest/invest-simulation-conf.json"

    val jsonStr = FileUtils.readFileToString(new File(str))

    val gson = new Gson();
    val test = new RegularInvestSimulationConf()
    val task : RegularInvestSimulationConf = gson.fromJson(jsonStr,test.getClass);
    import scala.collection.JavaConversions._
    for(test <- task.getTasks) {
      println(test.getApply)
    }
  }


}