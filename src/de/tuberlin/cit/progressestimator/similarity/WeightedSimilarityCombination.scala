package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

class WeightedSimilarityCombination (val simResponse: SimulationResponse, val power : Double) extends Similarity {
  
  val similarities = new ListBuffer[(Integer, Similarity, Boolean)]
  KEEP_THRESHOLD = 0.6
  def getIdentifier() : String = {
    val id = "WeightedSimilarityCombination[" + similarities.map(t => t._1 + "-" + t._2.getIdentifier).mkString(";") + "]"
    id
  }
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double) = {
    val t = getSimilarityWithVector(job1, job2, currentIteration, job2AdjustedRuntimes)
    return (t._1, t._2)
  }
  def getVectorSimIds() : ListBuffer[String] = {
    similarities.filter(_._3).map(_._2.getIdentifier())
  }
  def getSimilarityWithVector(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double, Array[Double]) = {
    val totalWeight = getTotalWeight()
    var keep = true
    val similarityTuples = similarities.map(t => { 
      val s = t._2.getSimilarity(job1, job2, currentIteration, job2AdjustedRuntimes)
      val sim = Math.pow(s._2, power)
      val useForWeightSolver = t._3
      if(!s._1) {
        simResponse.addSimilarityLog(job2, "[Discarded] T=" + t._2.KEEP_THRESHOLD)
      }
      (s._1, sim, t._1, useForWeightSolver)
    })
    val similarityVector = similarityTuples.filter(_._4).map(_._2).toArray
    var similarityS = similarityTuples.map(s => { 
      keep = keep && s._1
      s._2 * s._3 
    }).sum
    val similarity = similarityS / totalWeight
//    foldLeft[Double](0)((p, tuple) => { 
//             p + tuple._2.getSimilarity(job1, job2) * tuple._1 / totalWeight 
//             })
    keep = keep && similarity >= this.KEEP_THRESHOLD
    simResponse.addSimilarityLog(job2, s"Total Weighted Similarity: ", "%.4f".format(similarity))
    
    return (keep, similarity, similarityVector)
  }
  
  def getTotalWeight() = similarities.map(_._1.toInt).sum
  
  def addWeightedSimilarity(weight: Integer, similarity : Similarity, useForWeightSolver : Boolean = true) : Unit = {
    similarities += ((weight, similarity, useForWeightSolver))
  }
  
  def lowerThresholds(factor : Double) = {
    KEEP_THRESHOLD *= factor
    similarities.foreach(_._2.KEEP_THRESHOLD *= factor)
  }
  
}