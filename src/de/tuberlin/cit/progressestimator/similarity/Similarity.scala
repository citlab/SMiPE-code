package de.tuberlin.cit.progressestimator.similarity

import de.tuberlin.cit.progressestimator.entity.JobExecution;
import scala.collection.mutable.ListBuffer

trait Similarity {
  var KEEP_THRESHOLD = .8
  def getIdentifier() : String
  def getSimilarity(job1 : JobExecution, job2 : JobExecution, currentIteration : Int, job2AdjustedRuntimes : ListBuffer[Double] = ListBuffer[Double]()) : (Boolean, Double)
}