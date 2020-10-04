import java.io.{PrintWriter, File}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source._

object task1 {

  case class Review_Class(review_id: String, user_id: String, business_id: String, stars: String, text: String, date: String)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().appName("task1").config("spark.master", "local[*]").getOrCreate()

    val context = sparkSession.sparkContext
    context.setLogLevel("ERROR")
    val inputFile = args(0)
    val outputFile = args(1)
    val stopWords = args(2)
    val y = args(3)
    val m = args(4)
    val n = args(5)

    val lines = fromFile(stopWords).getLines.toArray
    val list_stop = lines
    val strip_l = list_stop.map(_.trim)

    val rdd = context.textFile(inputFile).
      map(extractData).map(x => (x.business_id, x.date, x.review_id, x.stars, x.text, x.user_id))

    val part_A_ans = part_A(rdd)
    val part_B_ans = part_B(rdd, y)
    val part_C_ans = part_C(rdd)
    val part_D_ans = part_D(rdd, m)
    val part_E_ans = part_E(rdd, n, strip_l)
    process_output(part_A_ans, part_B_ans, part_C_ans, part_D_ans, part_E_ans, outputFile)
  }

  def process_output(partA: Long, partB: Long, partC: Long, partD: Array[(String, Int)], partE: Array[(String, Int)], output: String) = {
    println(partA, partB, partC, partD.mkString(""), partE.mkString(""))
    val writerObject = new PrintWriter(new File(output))
    writerObject.write('{')
    writerObject.write("\"A\": " + partA + ", ")
    writerObject.write("\"B\": " + partB + ", ")
    writerObject.write("\"C\": " + partC + ", ")
    writerObject.write("\"D\": [")
    for (i <- partD) {
      writerObject.write("[\"" + i._1 + "\", " + i._2 + "]")
      if (partD.indexOf(i) < partD.length - 1) {
        writerObject.write(",")
      }
    }
    writerObject.write("],")
    writerObject.write("\"E\": [")
    for (i <- partE) {
      writerObject.write("\"" + i._1 + "\"")
      if (partE.indexOf(i) < partE.length - 1) {
        writerObject.write(", ")
      }
    }
    writerObject.write("]}")
    writerObject.close()

  }

  def reformat(word: String, stop: Array[String]): String = {

//    val rem = word.replaceAll("[\\t\\n]", " ").trim()

    val remove_sp_ch = word.replaceAll("[.,!?;:)\\](\\[)]", "").trim()


    if (stop.contains(remove_sp_ch)) {
      return ""
    }
    else {
      return remove_sp_ch
    }
  }

  def part_A(rdd: RDD[(String, String, String, String, String, String)]): Long = {
    val rdd1 = rdd.map(x => (x._3))
    return rdd1.count()
  }

  def part_B(rdd: RDD[(String, String, String, String, String, String)], y: String): Long = {
    val rdd1 = rdd.map(x => (x._3, x._2))
    val rdd2 = rdd1.map(x => x._2)
    val rdd_filter_year = rdd2.filter(x => x.contains(y))
    return rdd_filter_year.count()
  }

  def part_C(rdd: RDD[(String, String, String, String, String, String)]): Long = {
    val rdd1 = rdd.map(x => x._1).distinct()
    return rdd1.count()
  }

  def part_D(rdd: RDD[(String, String, String, String, String, String)], m: String): Array[(String, Int)] = {
    val rdd1 = rdd.map(x => (x._6, x._3))
    val rdd_pair_kv = rdd1.map(x => (x._1, 1))
    val rdd_reduce = rdd_pair_kv.reduceByKey(_ + _).sortByKey(ascending = true)
    val top_m = rdd_reduce.takeOrdered(Integer.parseInt(m))(Ordering.by(x => (-x._2, x._1)))
    return top_m
  }

  def part_E(rdd: RDD[(String, String, String, String, String, String)], n: String, stop: Array[String]): Array[(String, Int)] = {
    val rdd1 = rdd.map(x => (x._5))

    val rdd_flat = rdd1.flatMap(x => x.toLowerCase.split(" "))
    val rdd_pair = rdd_flat.map(x => (reformat(x, stop), 1))
    val rdd_filter = rdd_pair.filter(x=>x._1.length!=0)
    val rdd_reduce = rdd_filter.reduceByKey(_ + _).sortByKey(ascending = true)
    val top_m = rdd_reduce.takeOrdered(Integer.parseInt(n))(Ordering.by(x => (-x._2, x._1)))
    return top_m
  }

  def extractData(line: Any): Review_Class = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(line+"").extract[Review_Class]
  }
}
