package scala.test

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.rdd.RDD
import com.holdenkarau.spark.testing.RDDComparisons

@RunWith(classOf[JUnitRunner])
class TestSpark extends FlatSpec with Matchers with SharedSparkContext {

  behavior of "Words counter"

  it should "count words in Text" in {
    val test: String = "Hi Ashish Ashish will get job in Scala definetly Scala"
    val rdd = sc.parallelize(List(test), 2)
    val expected: RDD[(String, Int)] = rdd.flatMap { x => x.split(" ") }.map { x => (x, 1) }.reduceByKey(_ + _)
//    println(expected.collect().toList)
    val input: RDD[(String, Int)] = sc.parallelize(List(("Ashish", 2), ("will", 1), ("get", 1), ("Scala", 2), ("definetly", 1), ("job", 1), ("in", 1), ("Hi", 1)))
    assert(None === RDDComparisons.compare(expected, input))
  }

}