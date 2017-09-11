package de.tuberlin.cit.progressestimator.costfactor
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer

/**
 * This represents a cost factor,
 * which compares the iteration of two execution and infers
 * a factor representing the differences.
 */
trait CostFactor {
  /**
   * returns the cost factor for a given iteration for the two given jobs, also considering the given
   * adjusted runtimes for the second job.
   */
  def getCostFactor(job1 : JobExecution, job2 : JobExecution, iteration : Integer, adjustedRuntimes : ListBuffer[Double]) : Double;
}