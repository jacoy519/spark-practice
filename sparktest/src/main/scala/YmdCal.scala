import breeze.linalg.sum
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

case class Persons(id_person: Int, name: String, address: String)
case class Orders(id_order: Int, orderNum: Int, id_person: Int)

object YmdCal {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrameTest")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    /*
    val currentApplyShareDf = sqlContext.createDataFrame(List(Persons(1, "张三", "深圳"), Persons(2, "李四", "成都"), Persons(3, "王五", "厦门"), Persons(4, "朱六", "杭州")))
    val lastApplyShareDf = sqlContext.createDataFrame(List(Orders(1, 325, 2), Orders(2, 34, 3), Orders(3, 533, 1), Orders(4, 444, 1), Orders(5, 777, 11)))

    currentApplyShareDf.join(lastApplyShareDf, currentApplyShareDf("fund_code") <=> lastApplyShareDf("fund_code")&&
      currentApplyShareDf("regular_invest_type") <=> lastApplyShareDf("regular_invest_type")&&
      currentApplyShareDf("strategy_type") <=> lastApplyShareDf("strategy_type")&&
      currentApplyShareDf("stock_avg_day") <=> lastApplyShareDf("stock_avg_day"), "inner")
    currentApplyShareDf.ex*/

    val newCreditId = getNewCreditUserId(sqlContext);

    val memberLogonDf = getMemberLogonInfoDf(sqlContext);

    val joinDf = memberLogonDf.join(newCreditId,Seq("partyno"), "inner");

    val filterJoinDf = joinDf.rdd.filter( row => {
      val regist_at = row.getAs[String]("regist_at")
      val created_at = row.getAs[String]("created_at");
      val logon_at = row.getAs[String]("logon_at");
      logon_at.compare(created_at) >=0 && logon_at.compare(regist_at) <=0
    }).map(r => {

    })
  }


  //Todo
  def getCreditIdDf(sqlC:HiveContext) :DataFrame = {
    val sql =
      s"""
         |select
         |id,
         |user_id,
         |created_at
         |from pd_idl.member_credit_id
       """.stripMargin
    return sqlC.sql(sql);
  }

  //Todo
  def getYmdCurrentDf(sqlC:HiveContext) :DataFrame = {
    val sql =
      s"""
         |select
         |id,
         |user_id,
         |created_at
         |from sn_cdl.ymd_feature
       """.stripMargin
    return sqlC.sql(sql);
  }


  def getMemberLogonInfoDf(sqlC:HiveContext) :DataFrame = {
    val sql =
      s"""
         |select
         |li_partyno as partyno,
         |cid,
         |fp,
         |_g,
         |created_at as logon_at
         |from pd_idl.member_logon_info
       """.stripMargin
    return sqlC.sql(sql)
  }


  def getNewCreditUserId(sqlC: HiveContext): DataFrame = {
    val creditIdDf = getCreditIdDf(sqlC);
    val ymdCurrentDf = getYmdCurrentDf(sqlC);

    val creditUserIdDf = creditIdDf.select("user_id").distinct();
    val ymdUserIdDf = ymdCurrentDf.select("user_id").distinct();
    val newUserId = creditUserIdDf.except(ymdUserIdDf);

    val newCreditRdd = newUserId.join(creditIdDf, Seq("user_id"), "inner").map(
      r => {
        val userId = r.getAs[java.math.BigDecimal]("user_id");
        val id = r.getAs[java.math.BigDecimal]("id");
        val createdAt = r.getAs[String]("created_at")
        (userId, (createdAt, id))
      }
    ).reduceByKey((val1,val2) =>{
      selectNewValue(val1,val2)
    }).map(v => (v._2._2, v._1, v._2._1))
    val targetDf =  sqlC.createDataFrame(newCreditRdd).toDF("id", "user_id", "created_at");
    val memberCustomerDf = getMemberCustomer(sqlC);
    return targetDf.join(memberCustomerDf, Seq("user_id"), "inner");
  }

  def getMemberCustomer(sqlC:HiveContext) : DataFrame = {
    val sql =
      s"""
         |select
         |distinct
         |user_id,
         |party_no,
         |created_at as regist_at
         |from pd_idl.member_customer;
       """.stripMargin
    return sqlC.sql(sql);
  }



  def selectNewValue(val1:(String, java.math.BigDecimal), val2:(String, java.math.BigDecimal)): (String, java.math.BigDecimal) = {

    if(val1._1.compare(val2._1) <0) {
      return val1;
    }
    if(val1._1.compareTo(val2._1)>0) {
      return val2;
    }
    if(val1._1.compare(val2._1) == 0) {
      if(val1._2.compareTo(val2._2)<0)  {
        return val1;
      }else {
        return val2;
      }
    }
    return val1;
  }




}