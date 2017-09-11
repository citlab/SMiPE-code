package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

class ProductSimilarity (val simResponse: SimulationResponse) extends Similarity {
  
  val similarities = new ListBuffer[Similarity]
  KEEP_THRESHOLD = 0.6
  def getIdentifier() : String = {
    val id = "ProductSimilarity[" + similarities.map(t => t.getIdentifier).mkString(";") + "]"
    id
  }
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    val enableAfter = if(simResponse.addSimilarityLogs) {
      simResponse.addSimilarityLogs = false
      true
    } else false
    val sim = similarities.map(s => s.getSimilarity(job1, job2, currentIteration, job2AdjustedRuntimes)._2).product
    if(enableAfter) {
      simResponse.addSimilarityLogs = true
    }
    return (sim >= KEEP_THRESHOLD, sim)
  }
  
  def addSimilarity(similarity : Similarity) : ProductSimilarity = {
    similarities += similarity
    
    this
  }
  
}