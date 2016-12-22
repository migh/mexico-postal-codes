package com.migsar.postalCodesExample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scala.collection.mutable.ArrayBuffer

class PostalCodes(input: String) {
    val conf = new SparkConf().setAppName("PostalCodes")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/CPdescarga.txt")

    // Removing headers
    val dataWithoutHeaders = textFile.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(2) else iter }

    val tknData = dataWithoutHeaders.map( line => {
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

    def distinctPostalCodeTypes():Array[String] = this.tknData.map( pCode => (pCode("Type")) ).distinct.collect

    def postalCodesByCity():RDD[_] = {
        // City and postal codes to tuple
        this.tknData.map( pCode => (pCode("City"), pCode("PostalCode")) )
        .aggregateByKey(ArrayBuffer[String]())(
            (acc, value) => (acc += value),
            (acc1, acc2) => (acc1 ++ acc2)
        )
        .map( (item) => (item._1, item._2.length) )
        .sortBy(
            (item) => (item._2),
            false
        )

        // postalCodesByCity.saveAsTextFile("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/Results")
    }
}
