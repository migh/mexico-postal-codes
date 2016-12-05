package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PostalCodes {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PostalCodes")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/CPdescarga.txt")

    // Removing headers
    val dataWithoutHeaders = textFile.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(2) else iter }

    val tokenizedFileData = dataWithoutHeaders.map( line => {
      val elements = line.split("\\|")
      var cityCode = if (elements.length == 15) elements(14) else null;
      Map(
        "PostalCode" -> elements(0),
        "Name" -> elements(1),
        "Type" -> elements(2),
        "Municipality" -> elements(3),
        "State" -> elements(4),
        "City" -> elements(5),
        "AdminPostalCode" -> elements(6),
        "EntityCode" -> elements(7),
        "PostalOfficeCode" -> elements(8),
        "NA" -> elements(9),
        "TypeCode" -> elements(10),
        "MunicipalityCode" -> elements(11),
        "MunSettlementCode" -> elements(12),
        "SettlementCategory" -> elements(13),
        "CityCode" -> cityCode
      )
    })

    val mapStep = tokenizedFileData.map( pCode => pCode("City"))
    // val postalCodes = tokenizedFileData.map( pCode => (pCode("PostalCode"), pCode("Municipality"), pCode("Name"), pCode("State")))
    val states = mapStep.distinct()
    // val codesByState = postalCodes.aggregateByKey( 0, pcElem => pcElem._2 )

    states.saveAsTextFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/Results")
  }
}
