import breeze.linalg.sum
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

object RegularInvestBenefitSimulation {


  def main(args: Array[String]): Unit = {
    var list = new  ListBuffer[(Int, String)];
    list.append((1, "123123"))
    list.append((2, "123123"))
    list.append((3, "123123"))

    val benefitDf = null;

    for(currentDate<- currentDate) {
      var targetDate = list.head._2;
      if(currentDate < targetDate) {
        //要去计算对应的benefitDF了
        
      }
    }

  }


}