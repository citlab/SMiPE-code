package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution;
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.entity.Iteration
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.util.WeightFunctions

/**
 * The Active Data Record Distributon compares active data records iteration-wise
 * and returns a value representing the deviations.
 * It has a normalized variant and can use a weight function for iteration weighting.
 */
class ActiveDataRecordDistributionSimilarity(val simResponse: SimulationResponse, normalize: Boolean = true) extends Similarity {

  KEEP_THRESHOLD = .9
  val KEEP_THRESHOLD_MIN = 0.85 // the minimal keep threshold

  /**
   * returns the unique id.
   */
  def getIdentifier(): String = {
    if (normalize) {
      "ActiveDataRecordDistributionNormalized," + weightFunctionId + "," + deviationFunctionId
    } else {
      "ActiveDataRecordDistribution," + weightFunctionId + "," + deviationFunctionId
    }
  }
  /**
   * returns the similarity value.
   */
  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {
    if (!recordDataExists(job1, job2, currentIteration)) {
      simResponse.addSimilarityLog(job2, "No ActiveDataRecord data available")
      return (true, 1);
    }
    val sim = new DistributionSimilarity(simResponse, i => i._1.input.numberOfRecords,
      normalize, simName = this.getClass.getSimpleName).setWeightFunction(weightFunction, weightFunctionId)
    sim.KEEP_THRESHOLD = Math.max(this.KEEP_THRESHOLD, this.KEEP_THRESHOLD_MIN)
    if (useDeltaDeviationFlag) {
      sim.useDeltaDeviation()
    }
    sim.getSimilarity(job1, job2, currentIteration)
  }

  /**
   * returns true if both job executions contain active record data.
   */
  private def recordDataExists(job1: JobExecution, job2: JobExecution, currentIteration: Int): Boolean = {
    job1.iterations.size >= currentIteration && job2.iterations.size >= currentIteration &&
      job1.iterations.take(currentIteration)
      .filter(i => i.input.numberOfRecords == null).length == 0 && job2.iterations.take(currentIteration)
      .filter(i => i.input.numberOfRecords == null).length == 0 && job1.iterations.apply(0).input.numberOfRecords > 0 && job2.iterations.apply(0).input.numberOfRecords > 0
  }
  
  var weightFunction = WeightFunctions.weightFunction1
  var weightFunctionId = "weightFunction1"
  /**
   * sets the weight function for iteration weighting.
   */
  def setWeightFunction(f: (Int, Int) => Double, weightFunctionId: String): ActiveDataRecordDistributionSimilarity = {
    weightFunction = f
    this.weightFunctionId = weightFunctionId
    this
  }
  
  private var useDeltaDeviationFlag = false
  private var deviationFunctionId = "standard-deviation"
  /**
   * call this function if the delta deviation method should be used.
   */
  def useDeltaDeviation() : Similarity = {
    useDeltaDeviationFlag = true
    deviationFunctionId = "delta-deviation"
    this
  }

}