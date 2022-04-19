package org.example

import breeze.plot._
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.correlation.{Covariance, PearsonsCorrelation}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.StatCounter

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

object riskMC {
    def main(args: Array[String]): Unit = {
        // Set up Spark session and instantiate risk class
        val spark = SparkSession.builder()
                .master("local[2]")
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
        val factorsReturns = factors.map(risk.twoWeekReturns).toArray.toSeq
        //Plot returns
        risk.plotDistribution(factorsReturns(1))
        risk.plotDistribution(factorsReturns(2))

        //Factor returns correlations
        val factorMat = risk.transposeFactors(factorsReturns)
        val factorCor = new PearsonsCorrelation(factorMat).getCorrelationMatrix.getData
        println(factorCor.map(_.mkString("\t")).mkString("\n"))

        //MC sampling
        val numTrials = 10
        val parallelism = 1
        val baseSeed = 1001L
        val trials = risk.computeSimulatedReturns(stocksReturns, factorsReturns, baseSeed,
            numTrials, parallelism)
        trials.show(5)
        trials.cache()

        //Calculate 10% (c)VaR (and 95% CI)
        val VaRPercent = 0.1
        val VaR = risk.valueAtRisk(trials, VaRPercent)
        val cVaR = risk.conditionalValueAtRisk(trials, VaRPercent)
        val VaRCI = risk.bootstrappedCI(trials, risk.valueAtRisk, VaRPercent, 100, 0.95)
        val cVaRCI = risk.bootstrappedCI(trials, risk.conditionalValueAtRisk, VaRPercent, 100, 0.95)
        println("10% VaR = " + VaR + " (CI = " + VaRCI + ")")
        println("10% cVaR = " + cVaR + " (CI = " + cVaRCI + ")")
    }
}

class riskMC(private val spark: SparkSession) extends java.io.Serializable {
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

    def instrumentReturn(instrument: Array[Double], trial: Array[Double]): Double = {
        var instrumentReturn = instrument(0)
        var i = 0

        while (i < trial.length) {
            instrumentReturn += trial(i) * instrument(i + 1) //+1 due to intercept in instrument
            i += 1
        }
        instrumentReturn
    }

    def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
        var totalReturn = 0.0

        for (instrument <- instruments) {
            totalReturn += instrumentReturn(instrument, trial)
        }
        totalReturn / instruments.size
    }

    def allReturnsMCS(seed: Long, numTrials: Int, instruments: Seq[Array[Double]],
                      factorMeans: Array[Double], factorCov: Array[Array[Double]]): Seq[Double] = {
        val rand = new MersenneTwister(seed) //Should be good enough here
        val mvn = new MultivariateNormalDistribution(rand, factorMeans, factorCov)

        val allReturnsMCS = new Array[Double](numTrials)
        for (i <- 0 until numTrials) {
            val trialFactorReturns = mvn.sample()
            val trialFeatures = featurize(trialFactorReturns)
            allReturnsMCS(i) = trialReturn(trialFeatures, instruments)
        }
        allReturnsMCS
    }

    def computeSimulatedReturns(stocksReturns: Seq[Array[Double]],
                                factorsReturns: Seq[Array[Double]],
                                seed: Long,
                                numTrials: Int,
                                parallelism: Int): Dataset[Double] = {
        val factorMat = transposeFactors(factorsReturns)
        val factorCov = new Covariance(factorMat).getCovarianceMatrix.getData
        val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray
        val factorFeatures = factorMat.map(featurize)
        val factorModels = stocksReturns.map(lm(_, factorFeatures))
        val factorWeights = factorModels.map(_.estimateRegressionParameters()).toArray

        val seeds = (seed until seed + parallelism)
        val seedDS = seeds.toDS().repartition(parallelism)
        seedDS.flatMap(allReturnsMCS(_, numTrials / parallelism,
            factorWeights, factorMeans, factorCov))
    }

    def valueAtRisk(trials: Dataset[Double], percent: Double): Double = {
        val quantiles = trials.stat.approxQuantile("value", Array(percent), 0.0)
        quantiles.head //Best of worst 5% to get what we want
    }

    def conditionalValueAtRisk(trials: Dataset[Double], percent: Double): Double = {
        val topLosses = trials.orderBy("value")
                .limit(math.max(trials.count().toInt * percent, 1).toInt)
        topLosses.agg("value" -> "avg").first()(0).asInstanceOf[Double] //Avg of worst 5%
    }

    def bootstrappedCI(trials: Dataset[Double],
                       VaRFun: (Dataset[Double], Double) => Double,
                       percent: Double,
                       numResamples: Int,
                       confLevel: Double): (Double, Double) = {

        //Resample and compute function
        val stats = (0 until numResamples).map { _ =>
            val resample = trials.sample(withReplacement = true, 1.0)
            VaRFun(resample, percent)
        }.sorted

        val lowerInd = (numResamples * (1 - confLevel) / 2 - 1).toInt
        val upperInd = math.ceil(numResamples * (1 - (1 - confLevel) / 2)).toInt

        (stats(lowerInd), stats(upperInd))
    }
}