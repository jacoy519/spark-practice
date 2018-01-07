import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

case class Persons(id_person: Int, name: String, address: String)
case class Orders(id_order: Int, orderNum: Int, id_person: Int)

object test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrameTest")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

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
}