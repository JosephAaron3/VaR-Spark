package org.example

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.io.File
import java.util.Locale
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.util.StatCounter
import breeze.plot._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.catalyst.plans.logical.Sample

object riskMC {
    def main(args: Array[String]): Unit = {
        // Set up Spark session and instantiate risk class
        val spark = SparkSession.builder()
                .master("local[1]")
                .appName("Risk")
                .getOrCreate()
        val risk = new riskMC(spark)


        val start = LocalDate.of(2009, 10, 23)
        val end = LocalDate.of(2014, 10, 23)
        val stocksDir = "src/main/resources/stocks/"
        val factorsDir = "src/main/resources/factors/"

        // Load data
        val stocksPath = new File(stocksDir)
        val stocksFiles = stocksPath.listFiles()
        val rawStocks = stocksFiles.iterator.flatMap { file =>
            try {
                Some(risk.readHistory(file))
            } catch {
                case e: Exception => None
            }
        }
        val rawFactors = Array("NYSEARCA%3AGLD.csv", "NASDAQ%3ATLT.csv", "NYSEARCA%3ACRED.csv").
                map(x => new File(factorsDir + x)).
                map(risk.readHistory)

        // Preprocessing
        val rawStocksFiltered = rawStocks.filter(_.size >= 260 * 5 + 10)
        val stocks = rawStocksFiltered.map(risk.trim(_, start, end))
                .map(risk.impute(_, start, end))
        val factors = rawFactors.map(risk.trim(_, start, end))
                .map(risk.impute(_, start, end))
        val stocksReturns = stocks.map(risk.twoWeekReturns).toArray.toSeq
        val factorsReturns = factors.map(risk.twoWeekReturns)
        val factorMat = risk.transposeFactors(factorsReturns)
        val factorFeatures = factorMat.map(risk.featurize)

        //Fit LR models
        val factorWeights = stocksReturns.map(risk.lm(_, factorFeatures))
                .map(_.estimateRegressionParameters()).toArray

        //Plot returns
        risk.plotDistribution(factorsReturns(1))
        risk.plotDistribution(factorsReturns(2))
    }
}

class riskMC(private val spark: SparkSession) {
    import spark.implicits._

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

    def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double] = {
        history.sliding(10).map { //10 day sliding window for price change
            window =>
                val next = window.last._2
                val prev = window.head._2
                (next - prev) / prev
        }.toArray
    }

    def transposeFactors(histories: Seq[Array[Double]]): Array[Array[Double]] = {
        val mat = new Array[Array[Double]](histories.head.length)
        for (i <- histories.head.indices) {
            mat(i) = histories.map(_(i)).toArray
        }
        mat
    }

    //TODO: Add better feature transforms
    def featurize(factorReturns: Array[Double]): Array[Double] = {
        val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
        val sqrtReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
        squaredReturns ++ sqrtReturns ++ factorReturns
    }

    def lm(instrument: Array[Double], factorMatrix: Array[Array[Double]]):
    OLSMultipleLinearRegression = {
        val regression = new OLSMultipleLinearRegression()
        regression.newSampleData(instrument, factorMatrix)
        regression
    }

    def plotDistribution(samples: Array[Double]): Figure = {
        val min = samples.min
        val max = samples.max
        val sd = new StatCounter(samples).stdev
        val bandwidth = 1.06 * sd * math.pow(samples.size, -0.2)
        val domain = Range.BigDecimal(min, max, (max - min) / 100).map(_.toDouble).toList.toArray
        val kd = new KernelDensity().setSample(samples.toSeq.toDS.rdd).setBandwidth(bandwidth)
        val densities = kd.estimate(domain)
        val f = Figure()
        val p = f.subplot(0)
        p += plot(domain, densities)
        p.xlabel = "Two week return"
        p.ylabel = "Density"
        f
    }
}