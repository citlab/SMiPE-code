package de.tuberlin.cit.progressestimator.costfactor

import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * The cost factor extractor is the component which calculates the
 * cost factors of all similar executions for the estimation of a current job.
 */
trait CostFactorExtractor {
  
  /**
   * calculates the cost factors of all given similar executions.
   * The cost factors are stored in the 
   * JobExecutionWithEstimationDetails instance of the respective execution.
   */
  def getCostFactors(currentJob : JobExecution, similarJobs: ListBuffer[JobExecutionWithEstimationDetails],
      currentIteration : Integer, simResponse : SimulationResponse)
}