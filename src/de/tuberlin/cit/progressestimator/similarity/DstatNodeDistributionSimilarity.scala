package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.{ Iteration, JobExecution }
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import java.time.Instant
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.time.ZoneOffset
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.util.WeightFunctions

/**
 * The Hardware Similarity in the Node Distribution variant.
 */
class DstatNodeDistributionSimilarity(val simResponse: SimulationResponse, val dstatCache: DstatCache, val property: String) extends Similarity {

  KEEP_THRESHOLD = .4
  def getIdentifier(): String = {
    "DstatNodeDistributionSimilarity," + property + "," + devFunctionId
  }

  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {

    if (job1.iterations(0).resourceConfig.numberOfNodes == job1.iterations(0).resourceConfig.numberOfNodes) {

      var timeEndJob1 = job1.iterations.apply(currentIteration).timeFinish
      var timeEndJob2 = job2.iterations.apply(currentIteration).timeFinish
      var timeStartJob1 = job1.iterations.apply(0).timeStart
      var timeStartJob2 = job2.iterations.apply(0).timeStart
      val job1Values = dstatCache.getValueOfAllNodesFromCache(property, job1, currentIteration, timeStartJob1, timeEndJob1).sorted
      val job2Values = dstatCache.getValueOfAllNodesFromCache(property, job2, currentIteration, timeStartJob2, timeEndJob2).sorted

      val deviations = job1Values.zip(job2Values).map(t => devFunction(t._1, t._2))
      val deviationAvg = deviations.sum / deviations.length

      val similarity = deviationAvg

      simResponse.addSimilarityLog(job2, s"Dstat NodeDistr Sim [$property]", "" + similarity)
      (similarity > KEEP_THRESHOLD, similarity)
    } else {
      return (true, 0)
    }
  }

  val devFunctionSBId = "devSB"
  val devFunctionSB: ((Double, Double) => Double) = (v1, v2) => {
    val smaller = Math.min(v1, v2)
    val bigger = Math.max(v1, v2)
    val dev = smaller / bigger
    dev
  }
  val devFunctionCpuId = "devCPU"
  val devFunctionCpu: ((Double, Double) => Double) = (v1, v2) => {
    val diff = Math.abs(v1 - v2)
    val dev = 1 - diff / 100
    dev
  }

  val devFunctionCpuSqId = "devCpuSq"
  val devFunctionCpuSq: ((Double, Double) => Double) = (v1, v2) => {
    val diff = Math.abs(v1 - v2)
    val dev = 1 - diff / 100
    dev * dev
  }

  var devFunction = devFunctionSB
  var devFunctionId = devFunctionSBId

  if (property.equals("idl")) {
    devFunction = devFunctionCpuSq
    devFunctionId = devFunctionCpuSqId
  }

}