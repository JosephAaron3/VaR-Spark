package org.example

import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.io.File
import java.util.Locale

object riskMC {
    def main(args:Array[String]): Unit ={
        /*
            val sc = SparkSession.builder()
                    .master("local[2]")
                    .appName("Test")
                    .getOrCreate()
            */
        val start = LocalDate.of(2009, 10, 23)
        val end = LocalDate.of(2014, 10, 23)
        val stocksDir = new File("src/main/resources/stocks")
        val files = stocksDir.listFiles()
        val allStocks = files.iterator.flatMap { file =>
            try{
                Some(readHistory(file))
            } catch {
                case e: Exception => None
            }
        }
        val rawStocks = allStocks.filter(_.size >= 260 * 5 + 10)

        val factorsPrefix = "src/main/resources/factors/"
        val rawFactors = Array("NYSEARCA%3AGLD.csv", "NASDAQ%3ATLT.csv", "NYSEARCA%3ACRED.csv").
                map(x => new File(factorsPrefix + x)).
                map(readHistory)
    }

    def readHistory(file: File): Array[(LocalDate, Double)] = {
        val formatter = DateTimeFormatter.ofPattern("d-MMM-yy", Locale.ENGLISH)
        val source = scala.io.Source.fromFile(file)
        val lines = source.getLines().toSeq
        lines.tail.map { line =>
            val cols = line.split(',')
            val date = LocalDate.parse(cols(0), formatter)
            val value = cols(4).toDouble
            (date, value)
        }.reverse.toArray
    }
}