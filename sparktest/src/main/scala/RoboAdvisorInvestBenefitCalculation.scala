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

    val personDataFrame = sqlContext.createDataFrame(List(Persons(1, "张三", "深圳"), Persons(2, "李四", "成都"), Persons(3, "王五", "厦门"), Persons(4, "朱六", "杭州")))
    val orderDataFrame = sqlContext.createDataFrame(List(Orders(1, 325, 2), Orders(2, 34, 3), Orders(3, 533, 1), Orders(4, 444, 1), Orders(5, 777, 11)))

    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person")).show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "inner").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "left").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "left_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "right").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "right_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "full").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "full_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "outer").show()
  }

  def calculateInvestBenefit(sqlC: HiveContext, startDate: String , endDate: String): Unit = {
    val startNetValueDf = getStartDateNetValueDf(sqlC, startDate);
    val endNetValueDf = getEndDateNetValueDf(sqlC, endDate);
    val investShareDf = getInvestShareDf(sqlC, startDate);
    val dividendDf = getDividendDf(sqlC, startDate, endDate);
    val investRecordDf = investShareDf.join(startNetValueDf, investShareDf("fund_code") <=> startNetValueDf("start_fund_code"), "inner")
      .join(endNetValueDf, investShareDf("fund_code") <=> endNetValueDf("end_fund_code"), "inner")
      .join(dividendDf,  investShareDf("fund_code") <=> dividendDf("dividend_fund_code"), "left_outer")

  }

  def convertToInvestBenefitDf(sqlC: HiveContext, investRecordDf : DataFrame, investDate: String, checkDate:String) : DataFrame= {
    val investBenefitRdd = investRecordDf.map(
      r => Row(
        r.getAs[java.math.BigDecimal]("group_id"),
        r.getAs[String]("fund_code"),
        r.getAs[java.math.BigDecimal]("share"),
        r.getAs[java.math.BigDecimal]("apply"),
        investDate,
        checkDate,
        r.getAs[java.math.BigDecimal]("start_net_value"),
        r.getAs[String]("start_net_value_date"),
        r.getAs[java.math.BigDecimal]("end_net_value"),
        r.getAs[String]("end_net_value_date"),
        calculateDividend(r),
        calculateBenefit(r),
        calculateAsset(r)
      )
    )
    val investBenefitDf : DataFrame= sqlC.createDataFrame(investBenefitRdd)
    val TotalInvestBenefitResult = investBenefitDf.groupBy("group_id").agg(.agg(max("age"), avg("salary")))
  }

  def calculateAsset(row: Row): java.math.BigDecimal = {
    val share = row.getAs[java.math.BigDecimal]("share");
    val netValue = row.getAs[java.math.BigDecimal]("end_net_value");
    return share.multiply(netValue);
  }

  def calculateBenefit(row: Row): java.math.BigDecimal = {
    val share = row.getAs[java.math.BigDecimal]("share");
    val apply = row.getAs[java.math.BigDecimal]("apply");
    val netValue = row.getAs[java.math.BigDecimal]("end_net_value");
    val dividend = calculateDividend(row);
    return share.multiply(netValue).add(dividend).subtract(apply).divide(apply);
  }

  def calculateDividend(row:Row) : java.math.BigDecimal = {
    val apply = row.getAs[java.math.BigDecimal]("share");
    val perDividend = row.getAs[java.math.BigDecimal]("per_dividend");
    return apply.multiply(perDividend);
  }


  def getInvestShareDf(sqlC: HiveContext, investDate: String) : DataFrame = {
    //Todo
    val sql = ""
    return sqlC.sql(sql);
  }

  def getStartDateNetValueDf(sqlC: HiveContext, startDate: String): DataFrame = {
    //Todo
    val sql = ""
    return sqlC.sql(sql).selectExpr(
      "fund_code as start_fund_code",
      "net_value as start_net_value",
      "net_value_date as start_net_value_date");
  }

  def getDividendDf(sqlC:HiveContext, startDate:String, endDate:String) : DataFrame = {
    //Todo
    val sql = "";
    return sqlC.sql(sql);
  }


  def getEndDateNetValueDf(sqlC: HiveContext, endDate: String): DataFrame = {
    val sql =""
    return sqlC.sql(sql).selectExpr(
      "fund_code as end_fund_code",
      "net_value as end_net_value",
      "net_value_date as end_net_value_date");
  }


}





}