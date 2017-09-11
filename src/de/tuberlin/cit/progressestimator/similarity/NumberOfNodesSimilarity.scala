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
class AvgNumberOfNodesSimilarity (val simResponse: SimulationResponse) extends Similarity {
  KEEP_THRESHOLD = .2
  def getIdentifier() : String = {
    "AvgNumberOfNodes" + "," + deviationFunctionId
  }
  private var useDeltaDeviationFlag = false
  private var deviationFunctionId = "standard-deviation"
  def useDeltaDeviation() : Similarity = {
    useDeltaDeviationFlag = true
    deviationFunctionId = "delta-deviation"
    this
  }
  
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    val job1Value = job1.avgNumberOfNodes
    val job2Value = job2.avgNumberOfNodes
    var smallerSize = Math.min(job1Value, job2Value)
    var biggerSize = Math.max(job1Value, job2Value)

    if(smallerSize == 0 || biggerSize == 0) {
      return (true, 1)
    }
    
    var similarity = Math.pow(smallerSize / biggerSize, 2)
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
    simResponse.addSimilarityLog(job2, "Avg NodeNumber", ""+ similarity)
    return (keep, similarity)
  }
  
  
}