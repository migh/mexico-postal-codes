package com.migsar.postalCodesExample

object PostalCodesApp {
  def main(args: Array[String]) {

    val pc = new PostalCodes("file:///C:/Users/migsar.santiesteban/playground/spark/postal-codes/CPdescarga.txt")

    println( pc.distinctPostalCodeTypes.mkString(", ") )

    println( pc.postalCodesByCity.getClass )
  }
}
