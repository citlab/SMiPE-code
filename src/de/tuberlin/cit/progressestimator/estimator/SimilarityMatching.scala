package de.tuberlin.cit.progressestimator.estimator

import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
/**
 * This component performs the similarity matching.
 * It selects similar executions for an estimation according to similarity criteria.
 */
trait SimilarityMatching {

  /**
   * returns the similar executions set, which determines which executions
   * were discarded by the matching algorithm.
   */
  def getSimilarJobs(simResponse: SimulationResponse,
                     currentJob: JobExecution,
                     allJobs: ListBuffer[JobExecutionWithEstimationDetails],
                     currentIteration: Int, forceManualWeights: Boolean): ListBuffer[JobExecutionWithEstimationDetails]
}