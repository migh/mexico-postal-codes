package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PostalCodes {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PostalCodes")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/CPdescarga.txt")
    val tokenizedFileData = textFile.flatMap( line => line.split("\\|") )
    val countPrep = tokenizedFileData.map( word => (word, 1) )
    val counts = countPrep.reduceByKey( (accumValue, newValue) => accumValue + newValue )
    val sortedCounts = counts.sortBy( kvPair => kvPair._2, false )

    sortedCounts.saveAsTextFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/Results")
  }
}
