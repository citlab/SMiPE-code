package de.tuberlin.cit.progressestimator.estimator
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * the Estimator trait provides the functionality for an estimation.
 */
trait Estimator {
  
  /**
   * returns an estimate of the remaining runtime of the given current job, currently at
   * the given iteration, using the given job filter, simulation response and similarity weights 
   * solver.
   * 
   * The job filter is a function allowing to filter certain executions from the job history.
   * The Simulation Response is a helper object collecting information during the estimation, mainly
   * for reporting and debugging.
   * The similarity weights solver is used for training of the weights.
   * 
   */
  def estimate(currentJob: JobExecution, currentIteration: Int, jobFilter : JobExecution => Boolean, 
                        simResponse:SimulationResponse,
                        simWeightsSolver: SimilarityWeightsSolver = null): Double 

}