package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.{ Iteration, JobExecution }
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.util.WeightFunctions

class RuntimeDistributionSimilarity(val simResponse: SimulationResponse, normalize: Boolean = true) extends Similarity {
  KEEP_THRESHOLD = .8

  def getIdentifier(): String = {
    "RuntimeDistribution," + weightFunctionId + "," + deviationFunctionId
  }

  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {
    val sim = new DistributionSimilarity(simResponse, i => i._1.runtime, normalize, simName = this.getClass.getSimpleName).setWeightFunction(weightFunction, weightFunctionId)
    sim.KEEP_THRESHOLD = this.KEEP_THRESHOLD
    if (useDeltaDeviationFlag) {
      sim.useDeltaDeviation()
    }
    sim.getSimilarity(job1, job2, currentIteration)
  }

  var weightFunction = WeightFunctions.weightFunction1
  var weightFunctionId = "weightFunction1"
  def setWeightFunction(f: (Int, Int) => Double, weightFunctionId: String): RuntimeDistributionSimilarity = {
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