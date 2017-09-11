package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractorImpl
import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractor
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails

class ExtremeRuntimeValuesSimilarity (val simResponse: SimulationResponse) extends Similarity {
  KEEP_THRESHOLD = 0.95
  def getIdentifier() : String = {
    val id = "ExtremeRuntimeValuesSimilarity"
    id
  }
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    
    
    val remainingIterations = job2.iterations.drop(currentIteration - 1 )
    
    var previousRuntime = -1.0
    var extremeValues = 0
    remainingIterations.foreach(i => {
      if(previousRuntime >= 0) {
        val factor = i.runtime / previousRuntime
        val maxFactor = if(previousRuntime < 1.3) 10 else 3
        if(factor > maxFactor) {
          extremeValues += 1
        }
      }
      previousRuntime = i.runtime
    })
    
    val similarity =  1.0 - Math.min(1, 2 * Math.pow(extremeValues, 2).toDouble / (remainingIterations.length -1 ))
    
    simResponse.addSimilarityLog(job2, s"Extreme Runtime Values", "%.4f".format(similarity))
    return(similarity > KEEP_THRESHOLD, similarity)
//    return(similarity > 0.95, similarity)
  }

}