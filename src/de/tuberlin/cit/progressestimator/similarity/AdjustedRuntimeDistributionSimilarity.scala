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

class AdjustedRuntimeDistributionSimilarity(val simResponse: SimulationResponse, normalize: Boolean = false) extends Similarity {

  KEEP_THRESHOLD = .6
  def getIdentifier(): String = {
    "AdjustedRuntimeDistribution," + weightFunctionId + "," + deviationFunctionId + (if(normalize) ",normalized" else "")
  }

  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {
    val sim = new DistributionSimilarity(simResponse, i => {
      if (i._2 != null && i._2.length > i._1.nr) {
        i._2.apply(i._1.nr)
      } else {
        i._1.runtime
      }
    }, normalize, simName = this.getClass.getSimpleName).setWeightFunction(weightFunction, weightFunctionId)
    if (useDeltaDeviationFlag) {
      sim.useDeltaDeviation()
    }
    val t = sim.getSimilarity(job1, job2, currentIteration, job2AdjustedRuntimes)
    // adjust filter
    (t._2 > KEEP_THRESHOLD, t._2)
  }

  // the weight function and id
  var weightFunction = WeightFunctions.weightFunction1
  var weightFunctionId = "weightFunction1"
  def setWeightFunction(f: (Int, Int) => Double, weightFunctionId: String): AdjustedRuntimeDistributionSimilarity = {
    weightFunction = f
    this.weightFunctionId = weightFunctionId
    this
  }

  private var useDeltaDeviationFlag = false
  private var deviationFunctionId = "standard-deviation"
  /**
   * call this function to use the delta deviation method.
   */
  def useDeltaDeviation(): Similarity = {
    useDeltaDeviationFlag = true
    deviationFunctionId = "delta-deviation"
    this
  }
  
}