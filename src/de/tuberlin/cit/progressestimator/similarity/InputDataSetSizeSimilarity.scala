package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * This class determines the similarity of two job executions according
 * to their input dataset sizes.
 * 
 */
class InputDataSetSizeSimilarity (val simResponse: SimulationResponse) extends Similarity {
  KEEP_THRESHOLD = .8
  def getIdentifier() : String = {
    "InputDataSetSize"  + "," + deviationFunctionId
  }
  private var useDeltaDeviationFlag = false
  private var deviationFunctionId = "standard-deviation"
  def useDeltaDeviation() : Similarity = {
    useDeltaDeviationFlag = true
    deviationFunctionId = "delta-deviation"
    this
  }
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    val job1Value = job1.input.numberOfRecords
    val job2Value = job2.input.numberOfRecords
    var smallerSize = Math.min(job1Value, job2Value)
    var biggerSize = Math.max(job1Value, job2Value)

    if(smallerSize == 0 || biggerSize == 0) {
        simResponse.addSimilarityLog(job2, "One Input Size is 0")
        simResponse.addSimilarityLog(job2, "InputDataSetSize Similarity", "1")
      return (true, 1)
    }
    var similarity = smallerSize.toDouble / biggerSize
    var keep = similarity > KEEP_THRESHOLD
     if(useDeltaDeviationFlag) {
      val delta = Math.abs(job1Value - job2Value)
      if(job1Value > 0) {
        similarity = 1  - (delta / job1Value)
        keep = similarity > KEEP_THRESHOLD
      } else {
        similarity = 0
        keep = true
      }
    }
    simResponse.addSimilarityLog(job2, "InputDataSetSize Similarity", ""+ similarity)
    return (keep, similarity)
  }
  
  
}