package de.tuberlin.cit.progressestimator.estimator.settransformation

import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * This represents a set transformation.
 * It is a function which is applied during the similarity matching to the final 
 * similar executions set.
 */
trait SimilarSetTransformation {
  
  /**
   * transform the given similar executions set (l)
   * used in the estimation of the given current job and current iteration.
   */
  def transform(l: ListBuffer[JobExecutionWithEstimationDetails], currentJob : JobExecution, currentIteration : Integer, simResponse : SimulationResponse) : Unit = {
    
  }
}