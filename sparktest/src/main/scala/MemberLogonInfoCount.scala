import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object MemberLogonInfoCount {




  def logonInfoCount(col:String,sqlC:HiveContext): Unit = {

    recordOld(col, sqlC);

    val memberLogonInfoDf = getMemberLogonInfo(col, sqlC);

    val memberLogonInfoCountDf = memberLogonInfoDf.groupBy("key_value").agg(("id","count")).selectExpr("key_value","count(id) as key_count");

    val totalRecordKeyDf = memberLogonInfoDf.select("key_value").distinct();

    val oldRecordDf = getOldRecordDf(col, sqlC);

    val oldRecordKeyDf = oldRecordDf.select("key_value").distinct();

    val newRecordKeyDf = totalRecordKeyDf.except(oldRecordKeyDf);

    val newRecordDf = newRecordKeyDf.join(memberLogonInfoCountDf, Seq("key_value"),"inner");

    val updateRecordRdd = oldRecordDf.join(memberLogonInfoCountDf, Seq("key_value"),"inner").map(r => {
      val key = r.getAs[String]("key_value");
      val oldCount = r.getAs[java.math.BigDecimal]("old_key_count")
      val newCount = r.getAs[java.math.BigDecimal]("new_Key_count")
      var finalValue = BigDecimal.apply(-1).bigDecimal;
      if(oldCount != null && newCount != null && !newCount.equals(oldCount)) {
        finalValue = newCount
      }
      (key, finalValue)
    }).filter( value => value._2.compareTo(BigDecimal.apply(0).bigDecimal) >0)

    val updateRecordDf = sqlC.createDataFrame(updateRecordRdd).toDF("key_value", "key_count");

    saveDfFrame(updateRecordDf,sqlC, "update", col)
    saveDfFrame(newRecordDf, sqlC, "new", col);
    saveDfFrame(memberLogonInfoCountDf, sqlC, "last", col)

  }

  def saveDfFrame(df:DataFrame, sqlC:HiveContext, pk:String, col:String): Unit = {
    val tmpTableName = s"tmp_record_${pk}"
    val tableName = getCalRecordTableName(col)
    val sql = s"insert overwrite table ${tableName} partition(partition_key='${pk}') select key_value, key_count from ${tmpTableName}";
    df.registerTempTable(tmpTableName)
    sqlC.sql(sql);
    sqlC.clearCache()
  }

  def getMemberLogonInfo(col:String, sqlC:HiveContext) : DataFrame = {
    val sql = s"select distinct id, ${col} as key_value from pd_idl.member_logon_info where ${col} is not null";
    return sqlC.sql(sql);
  }

  def getOldRecordDf(col:String, sqlC:HiveContext):DataFrame = {
    val tableName = getCalRecordTableName(col);
    val sql = s"select key_value, key_count as old_key_count from ${tableName} where partition_key='last'";
    return sqlC.sql(sql);
  }

  def recordOld(col:String ,sqlC:HiveContext): Unit = {
    val tableName = getCalRecordTableName(col)
    val sql = s"insert overwrite table ${tableName} partition(partition_key='last') select key_value, key_count from ${tableName} where partition_key = 'new'";
    sqlC.sql(sql);
  }


  def getCalRecordTableName(col:String) : String = {
    return s"sn_cdl.${col}_cal_record";
  }


}
