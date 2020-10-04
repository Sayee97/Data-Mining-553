import java.io.{File, PrintWriter}

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object task3 {
  case class Review_Class(review_id: String, user_id: String, business_id: String, stars: String, text: String, date: String)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().appName("task3").config("spark.master", "local[*]").getOrCreate()

    val spark_context = sparkSession.sparkContext
    spark_context.setLogLevel("ERROR")
    val inputFile = args(0)
    val outputFile = args(1)
    val partition_type = args(2)
    val n_partitions = args(3)
    val n = args(4)
    val rdd = spark_context.textFile(inputFile)
      .map(extractData)
      .map(x => (x.business_id, x.date, x.review_id, x.stars, x.text, x.user_id))
    var rdd_reqd = rdd.map(x => (x._1, 1))
    // custimized partition
    if (partition_type == "customized") {
      rdd_reqd = rdd_reqd.partitionBy(new HashPartitioner(Integer.parseInt(n_partitions)))
    }

    val n_partition = rdd_reqd.getNumPartitions
    val items = calc_items(rdd_reqd)
    val result = calc_more_than_n(rdd_reqd, Integer.parseInt(n))

    println(n_partition, items.mkString(","), result.mkString(","))
    process_output(n_partition, items, result, outputFile)
  }

  def calc_items(rdd: RDD[(String, Int)]): Array[Int] = {
    val rdd_rq = rdd.glom().map(_.length)
    return rdd_rq.collect()
  }

  def process_output(partA: Long, partB: Array[Int], partC: Array[(String, Int)], output: String) = {
    val writerObject = new PrintWriter(new File(output))
    writerObject.write('{')
    writerObject.write("\"n_partitions\": " + partA + ", ")
    writerObject.write("\"n_items\": [")
    for (i <- partB) {
      writerObject.write(i.toString)
      if (partB.indexOf(i) < partB.length - 1) {
        writerObject.write(", ")
      }
    }
    writerObject.write("], ")
    writerObject.write("\"result\": [")
    for (i <- partC) {
      writerObject.write("[\"" + i._1 + "\", " + i._2 + "]")
      if (partC.indexOf(i) < partC.length - 1) {
        writerObject.write(",")
      }
    }
    writerObject.write("]}")
    writerObject.close()
  }

  def calc_more_than_n(rdd: RDD[(String, Int)], n: Int): Array[(String, Int)] = {
    val rdd_red = rdd.reduceByKey(_ + _)
    val rdd_cal = rdd_red.filter(x => x._2 > n)
    return rdd_cal.collect()
  }

  def extractData(row: Any): Review_Class = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(row+"").extract[Review_Class]
  }
}

