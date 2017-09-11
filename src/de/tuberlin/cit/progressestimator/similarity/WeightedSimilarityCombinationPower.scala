package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

class WeightedSimilarityCombinationPower (val power: Double, val wrappedSimilarity : WeightedSimilarityCombination, val simResponse: SimulationResponse) extends Similarity {
  
  def getIdentifier() : String = {
    val id = "similarityPower[" + power + "][" + wrappedSimilarity.getIdentifier() + "]"
    id
  }
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    val t = getSimilarityWithVector(job1, job2, currentIteration, job2AdjustedRuntimes)
    return (t._1, t._2)
  }
  def getSimilarityWithVector(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double, Array[Double]) = {
    val v = wrappedSimilarity.getSimilarityWithVector(job1, job2, currentIteration, job2AdjustedRuntimes)
    val sim = Math.pow(v._2, power)
    return (v._1, sim, v._3)
  }
  def lowerThresholds(factor : Double) = {
    wrappedSimilarity.lowerThresholds(factor)
  }
  def getVectorSimIds() : ListBuffer[String] = {
    wrappedSimilarity.getVectorSimIds
  }

}