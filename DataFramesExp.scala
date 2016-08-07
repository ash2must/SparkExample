package spark.exp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.RangePartitioner

object DataFramesExp extends App {
  val conf = new SparkConf().setMaster("local[4]").setAppName("Socgen")
  val sc = new SparkContext(conf)
  val sqlCs = new SQLContext(sc)
  //  val x=sc.textFile("x", 5)
//  val pl = sc.parallelize(List("1", "2", "3"), 2)
//  println(pl.map { x => (x+1,1)}.partitionBy(new HashPartitioner(5)).collect().toList)
//  x.map { x => (x, 1) }.partitionBy(new HashPartitioner(5))
  //  x.pa
  val babyNames = sqlCs.read.format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load("/home/ashish/Downloads/BabyNames.csv")
  val cache = babyNames.cache()
//  val schema = cache.printSchema()
  cache.filter(cache("Year")>2010).show()
  val x = babyNames.createOrReplaceTempView("Baby")
  val ql=sqlCs.table("Baby")
  sqlCs.sql("Select distinct year from Baby")
  val pName=sqlCs.sql("Select distinct ('First Name'),count(County) as cnt from Baby group by 'First Name' order by cnt desc LIMIT 10")
//  pName.collect().foreach { println }
//  val r=x.rdd.map { x => (x(0),x(1)) }.saveAsHadoopFile("/home/ashish/Downloads/BBBB")
  
//  val count =x.rdd.distinct()
//  println(pl.map { x => (x,1)}.collect().toList)
//  val sa=pl.map { x => (x,1)}.saveAsHadoopFile("/home/ashish/Downloads/BBBB")
  //  map { x => (x, 1) }.reduceByKey(_ + _)
//  println(count.collect().toList)
  //  x.rdd.saveAsTextFile("/home/ashish/Downloads/BabyNamesYear")
  //  babyNames.write.csv("/home/ashish/Downloads/BabyNames1.csv")
  //  babyNames.collect().foreach { println }
}