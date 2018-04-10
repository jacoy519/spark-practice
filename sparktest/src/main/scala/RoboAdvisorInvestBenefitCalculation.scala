import breeze.linalg.sum
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

case class Persons(id_person: Int, name: String, address: String)
case class Orders(id_order: Int, orderNum: Int, id_person: Int)

object RoboAdvisorInvestBenefitCalculation {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrameTest")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    val currentApplyShareDf = sqlContext.createDataFrame(List(Persons(1, "张三", "深圳"), Persons(2, "李四", "成都"), Persons(3, "王五", "厦门"), Persons(4, "朱六", "杭州")))
    val lastApplyShareDf = sqlContext.createDataFrame(List(Orders(1, 325, 2), Orders(2, 34, 3), Orders(3, 533, 1), Orders(4, 444, 1), Orders(5, 777, 11)))

    currentApplyShareDf.join(lastApplyShareDf, currentApplyShareDf("fund_code") <=> lastApplyShareDf("fund_code")&&
      currentApplyShareDf("regular_invest_type") <=> lastApplyShareDf("regular_invest_type")&&
      currentApplyShareDf("strategy_type") <=> lastApplyShareDf("strategy_type")&&
      currentApplyShareDf("stock_avg_day") <=> lastApplyShareDf("stock_avg_day"), "inner")
  }

}