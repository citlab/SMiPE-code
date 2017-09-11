package de.tuberlin.cit.progressestimator.rest

import com.google.inject.Inject
import com.google.inject.Singleton
import de.tuberlin.cit.progressestimator.util.reports.SimilarityQualityReport
import de.tuberlin.cit.progressestimator.similarity._
import de.tuberlin.cit.progressestimator.simulator.SimulationResponseDummy
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReportDummy
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.EstimatorImpl
import scala.util.Random
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import java.time.format.DateTimeFormatter
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.estimator.Estimator
import java.time.ZoneOffset
import java.time.Instant
import de.tuberlin.cit.progressestimator.util.reports.Reports
import scala.collection.mutable.HashMap
import de.tuberlin.cit.progressestimator.estimator.DynamicSimilarityThresholdExtractor
import de.tuberlin.cit.progressestimator.util.WeightFunctions
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.AbstractBuffer

@Singleton
class SimilarityQualityService @Inject() (val dstatCache: DstatCache, val simThresholdExtractor: DynamicSimilarityThresholdExtractor,
                                          val reports: Reports, val jobHistory: JobHistory, val estimator: Estimator,
                                          val config: EstimatorConfig) {

  val responseDummy = new SimulationResponseDummy
  /**
   * computes the similarity quality and writes the report.
   */
  def serveSimilarityQuality() = {
    val algorithms = jobHistory.getJobExecutions().map(_.name).distinct.reverse
    algorithms.foreach(algo => {
      val datasets = jobHistory.getJobExecutions().filter(_.name.equals(algo)).map(_.input.name).distinct
      println("Similarity Quality for $algo")
      simQuality(algo)

      val datasetGranularity = false // calculate quality additional for each dataset
      if (datasetGranularity) {
        datasets.foreach(dataset => {
          simQuality(algo, dataset)
        })
      }
    })

  }

  /**
   * compute quality using all executions of the given algorithm
   */
  private def simQuality(algo: String) = {
    val usedJobs = jobHistory.getJobExecutions().filter(j => {
      j.finished && j.name.equals(algo)
    })
    similarityQualityHelper(algo + "-all", usedJobs)
  }

  /**
   * compute quality using all executions of the given algorithm and dataset
   */
  private def simQuality(algo: String, dataset: String) = {
    val usedJobs = jobHistory.getJobExecutions().filter(j => {
      j.finished && j.name.equals(algo) && j.input.name.equals(dataset)
    })
    similarityQualityHelper(algo + "-" + dataset, usedJobs)
  }
  private def similarityQualityHelper(name: String, allJobs: ListBuffer[JobExecution]) = {
    try {
      val criteria = simQualitySimilarities(responseDummy)
      val report = new SimilarityQualityReport(config)
      val points = HashMap.empty[String, ArrayBuffer[(Double, Double)]]
      var i = 0
      for (j <- allJobs) {
        i += 1
        val others = allJobs.filter(_.name.equals(j.name)).filter(o => j.id > o.id)
        for (o <- others) {
          val iterations = List.range(0, 20).map(_ * 4 + 3).toSeq // Seq(25, 20, 15, 10, 5) // TODO choose x in equally-spaced distances, with additional padding at end and beginning
          for (i <- iterations) {
            qualitySimulation(j, o, i, criteria, points)
          }
        }
      }

      createHistogramsAndWriteReport(name, points, report)
      estimator.asInstanceOf[EstimatorImpl].bellToDisk()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
  private def qualitySimulation(j: JobExecution, o: JobExecution, i: Int, criteria: Seq[(String, Similarity)], points: HashMap[String, ArrayBuffer[(Double, Double)]]) = {
    if (j.iterations.length > i && o.iterations.length > i) {
      val responseDummyE = new SimulationResponseDummy
      val estimates = estimator.asInstanceOf[EstimatorImpl].estimateWithGivenJobs(j, (new ListBuffer[JobExecution]()).+=(o), i, responseDummyE)

      val remainingPrediction = estimates(0).prediction
      val remainingActual = j.iterations.drop(i).map(_.runtime).sum
      if (remainingPrediction != 0 || remainingActual != 0) {
        val accuracy = Math.max(0, 1 - (Math.abs(remainingPrediction - remainingActual) / remainingActual))
        if (!java.lang.Double.isNaN(accuracy)) {
          for (t <- criteria) {
            val c = t._2
            if (!points.contains(t._1)) {
              points.put(t._1, new ArrayBuffer[(Double, Double)])
            }
            //                val points = new ListBuffer[(Double, Double)]
            val (keep, similarity) = c.getSimilarity(j, o, i, estimates(0).adjustedTimes)

            var sim = similarity
            if (sim > 1) sim = 1
            if (sim < 0) sim = 0
            if (sim.equals(Double.NaN)) sim = 0

            points.get(t._1).get += ((sim, accuracy))
            //              println(s"($sim, $remainingSim)")
          }
        }
      }
      responseDummyE.removeLogs()
      responseDummy.removeLogs()
    }
  }

  private def createHistogramsAndWriteReport(name: String, points: HashMap[String, ArrayBuffer[(Double, Double)]], report: SimilarityQualityReport) = {
    val csv = (new Array[String](105)).map(s => "")
    val jobName = name
    val maxPoints = 1200
    val r = Random
    for (t <- criteria) {
      if (points.get(t._1).isDefined) {
        var allPointsUnfiltered = points.get(t._1).get
        var allPoints = allPointsUnfiltered.filter(p => p._1 >= 0 && p._2 <= 1)

        var cPoints = allPoints
        if (cPoints.size > maxPoints) {
          // sample! too many points for the browser graph
          val keepPointsPercentage = maxPoints.toFloat / cPoints.size
          cPoints = cPoints.filter(_ => r.nextFloat() < keepPointsPercentage)
        }
        cPoints += ((0, 0), (1, 1)) // fix scale
        report.createScatterGraph(Seq((t._1, cPoints)))
        createPointsHistogram(t._1, allPoints, report, t._2, jobName, csv)
      }
    }

    val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm").withZone(ZoneOffset.ofHours(2)).format(Instant.now())
    val fileName = "similarityQuality-" + timestamp + "-" + name + ".html"
    reports.writeFile(report.createReport(), fileName)
    val fileNameCSV = "similarityQuality-" + timestamp + "-" + name + ".csv"
    val csvString = csv.filter(s => !s.equals("")).mkString("\r\n")
    reports.writeFile(csvString, fileNameCSV)
    println(s"Written SimQuality Report: $fileName")
  }
  private def createPointsHistogram(name: String, points: AbstractBuffer[(Double, Double)], report: SimilarityQualityReport, sim: Similarity, jobName: String, csv: Array[String]) = {

    val histogram = ListBuffer[(Double, Double)]()
    val pointShare = ListBuffer[(Double, Double)]()
    val pointsCount = points.length
    var currentSum = 0.0;
    var currentCount = 0;
    var unweightedSum = 0.0;
    var unweightedCount = 0;
    var step = 1.0;
    var previousStep = 1.0;

    val simId = sim.getIdentifier()
    csv(0) += "\t" + "H#" + simId + "\t" + "C#" + simId + "\t" + "S#" + simId
    for (i <- 1 to 100) {
      previousStep = previousStep - .01
      val thisStepPoints = points.filter(p => p._1 > previousStep && p._1 <= step).map(_._2)
      val thisSum = thisStepPoints.sum
      val thisCount = thisStepPoints.length
      var thisAvg = .0
      if (thisCount != 0) {
        thisAvg = thisSum / thisCount
        unweightedSum = unweightedSum + thisAvg
        unweightedCount = unweightedCount + 1
      }
      currentSum = currentSum + thisSum
      currentCount = currentCount + thisCount
      var value = .0
      if (currentCount != 0) {
        value = currentSum / currentCount
      }
      var valueUnw = .0
      if (unweightedCount != 0) {
        valueUnw = unweightedSum / unweightedCount
      }
      histogram += ((previousStep, value))
      val countV = currentCount.toDouble / pointsCount
      pointShare += ((previousStep, countV))
      if (csv(101 - i).equals("")) {
        csv(101 - i) = "%.2f".format(step)
      }
      csv(101 - i) += "\t" + "%.6f".format(value) + "\t" + "%.6f".format(countV)

      step = step - .01
    }

    val normalizedQuality = createNormalizedQuality(histogram, pointShare, csv)
    if (jobName != null) {
      simThresholdExtractor.addHistogramToCache(jobName, sim, histogram.reverse.toArray, pointShare.reverse.toArray)
    }
    report.createScatterGraph(Seq((" Histogram", histogram.reverse), (" Point Share", pointShare.reverse), (" Normalized Quality", normalizedQuality)), float = false, add0011 = true)

  }

  private def createNormalizedQuality(histogram: ListBuffer[(Double, Double)], pointShare: ListBuffer[(Double, Double)], csv: Array[String]): ListBuffer[(Double, Double)] = {
    val normalizedQuality = ListBuffer[(Double, Double)]()
    for (i <- 1 to 100) {
      val step = .01 * i

      //      val vs = scatterGraphCount.zip(scatterGraphUnweighted).filter(t => {
      val vs = pointShare.zip(histogram).filter(t => {
        val count = t._1._2

        count >= 1.0 - step
      }).map(t => t._2._2).take(1)
      val value = if (vs.length > 0) {
        vs.apply(0)
      } else {
        0.0
      }

      normalizedQuality += ((step, value))
      csv(i) += "\t" + "%.6f".format(value)

    }

    normalizedQuality
  }

  var criteria: Seq[(String, Similarity)] = null
  private def simQualitySimilarities(responseDummy: SimulationResponseDummy): Seq[(String, Similarity)] = {
    if (criteria == null) {
      val dummyReport = new SimilarityReportDummy()


    var dstatDistrCpu = new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "idl")
    val dstatDistrIo = new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "writ")
    val dstatDistrSend = new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "send")
    val dstatDistrRecv = new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "recv")
    var inputSizeSimilarity = new InputDataSetSizeSimilarity(responseDummy)
    var runtimeSimilarityAdjusted = (new AdjustedRuntimeDistributionSimilarity(responseDummy)).setWeightFunction(WeightFunctions.weightFunction1OverX, "weightFunction1OverX").useDeltaDeviation()
    val adjustedRuntimeWithInput = new ProductSimilarity(responseDummy).addSimilarity(runtimeSimilarityAdjusted).addSimilarity(inputSizeSimilarity)
    var activeRecordDistributionSimilarity = new ActiveDataRecordDistributionSimilarity(responseDummy, normalize = false).useDeltaDeviation()
    val adjustedRuntimeWithActiveRecordWithInput = new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithInput).addSimilarity(activeRecordDistributionSimilarity)
      criteria = Seq(

        // Runtime Similarity Variants
        ("Abs. RuntimeDistribution", new RuntimeDistributionSimilarity(responseDummy, false).useDeltaDeviation()),
        ("Adjusted RuntimeDistribution", (new AdjustedRuntimeDistributionSimilarity(responseDummy)).useDeltaDeviation()),
        ("Adjusted RuntimeDistribution WeightedInverse", (new AdjustedRuntimeDistributionSimilarity(responseDummy)).setWeightFunction(WeightFunctions.weightFunction1OverX, "weightFunction1OverX").useDeltaDeviation()),
        ("Adjusted RuntimeDistribution WeightedGaussian", (new AdjustedRuntimeDistributionSimilarity(responseDummy)).setWeightFunction(WeightFunctions.weightFunctionGaussian, "weightFunctionGaussian").useDeltaDeviation()),
        ("Adjusted RuntimeDistribution WeightedInverseSqrt", (new AdjustedRuntimeDistributionSimilarity(responseDummy)).setWeightFunction(WeightFunctions.weightFunction1OverSquareRootX, "weightFunction1OverSqrtX").useDeltaDeviation()),
        ("Adjusted RuntimeDistribution WeightedLinear", (new AdjustedRuntimeDistributionSimilarity(responseDummy)).setWeightFunction(WeightFunctions.weightFunctionLinear, "weightFunctionLinear").useDeltaDeviation()),
        ("Adjusted RuntimeDistribution Nomalized", (new AdjustedRuntimeDistributionSimilarity(responseDummy, normalize = true)).useDeltaDeviation()),

        // ACtive Records Similarity Variants
        ("Abs. ActiveDataRecordDistribution", (new ActiveDataRecordDistributionSimilarity(responseDummy, false)).useDeltaDeviation()),
        ("Norm. ActiveDataRecordDistribution", (new ActiveDataRecordDistributionSimilarity(responseDummy, true)).useDeltaDeviation()),

        // Hardware Similarity Variants
        ("Dstat Distribution CPU", new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "idl")),
        ("Dstat Distribution IO", new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "writ")),
        ("Dstat Distribution Send", new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "send")),
        ("Dstat Distribution Recv", new DstatNodeDistributionSimilarity(responseDummy, dstatCache, "recv")),
        ("Dstat CPU idl", (new DstatSimilarity(responseDummy, "idl", dstatCache)).useDeltaDeviation()),
        ("Dstat IO writ", (new DstatSimilarity(responseDummy, "writ", dstatCache)).useDeltaDeviation()),
        ("Dstat network recv", (new DstatSimilarity(responseDummy, "send", dstatCache)).useDeltaDeviation()),
        ("Dstat network send", (new DstatSimilarity(responseDummy, "recv", dstatCache)).useDeltaDeviation()),

        // Input Similarity
        ("InputSize", new InputDataSetSizeSimilarity(responseDummy)),

        // Scale-Out Similarity
        ("AvgNodeNumber", new AvgNumberOfNodesSimilarity(responseDummy)),
        
        
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrCpu)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrSend)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrRecv)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrIo)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrCpu)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrSend)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrRecv)),
        ("c", new ProductSimilarity(responseDummy).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrIo))
        )

    }

    criteria

  }

}