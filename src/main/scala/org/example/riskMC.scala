package org.example

import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.io.File
import java.util.Locale
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps

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
        val stocksDir = "src/main/resources/stocks/"
        val factorsDir = "src/main/resources/factors/"

        // Load data
        val stocksPath = new File(stocksDir)
        val stocksFiles = stocksPath.listFiles()
        val rawStocks = stocksFiles.iterator.flatMap { file =>
            try{
                Some(readHistory(file))
            } catch {
                case e: Exception => None
            }
        }
        val rawFactors = Array("NYSEARCA%3AGLD.csv", "NASDAQ%3ATLT.csv", "NYSEARCA%3ACRED.csv").
                map(x => new File(factorsDir + x)).
                map(readHistory)

        // Preprocessing
        val rawStocksFiltered = rawStocks.filter(_.size >= 260 * 5 + 10)
        val stocks = rawStocksFiltered.map(trim(_, start, end)).map(impute(_, start, end))
        val factors = rawFactors.map(trim(_, start, end)).map(impute(_, start, end))
        val T = stocks.next.length
        println((stocks ++ factors).forall(_.size == T)) //Check all histories are equal length
    }

    /**
     * Extract the date and price of a historic stock CSV
     *
     * @param file from stock price CSV, date in 1st column, closing price in 3rd column
     * @return Array of key-value pairs for date, (closing) price
     */
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

    def trim(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate):
    Array[(LocalDate, Double)] = {
        var trimmed = history
                .dropWhile(_._1.isBefore(start)) //Drop prices before start
                .takeWhile(x => x._1.isBefore(end) || x._1.isEqual(end)) //Keep prices before end
        if (trimmed.head._1 != start) {
            trimmed = Array((start, trimmed.head._2)) ++ trimmed //Add synthetic row for start date
        }
        if (trimmed.last._1 != end) {
            trimmed = trimmed ++ Array((end, trimmed.last._2)) //Add synthetic row for end date
        }
        trimmed
    }

    def impute(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate):
    Array[(LocalDate, Double)] = {
        var current = history
        val filled = new ArrayBuffer[(LocalDate, Double)]()
        var currentDate = start
        while (currentDate.isBefore(end)) {
            if (current.tail.nonEmpty && current.tail.head._1 == currentDate) {
                current = current.tail
            }
            filled += ((currentDate, current.head._2))
            currentDate = currentDate.plusDays(1)
            if (currentDate.getDayOfWeek.getValue > 5) { //Skip weekends
                currentDate = currentDate.plusDays(2)
            }
        }
        filled.toArray
    }
}