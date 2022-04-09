package org.example

import org.apache.spark.sql.SparkSession

object riskMC {

  def main(args:Array[String]): Unit ={

    val sc = SparkSession.builder()
            .master("local[2]")
            .appName("Test")
            .getOrCreate()

    println("SparkContext details:")
    println("Name :" + sc.sparkContext.appName)
    println("Mode :" + sc.sparkContext.deployMode)
    println("Master :" + sc.sparkContext.master)

  }
}