package de.tuberlin.cit.progressestimator.costfactor
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer

/**
 * The iteration factor infers a cost factor comparing the 
 * runtimes of two iterations. 
 */
class IterationRuntimeCostFactor extends CostFactor {
  def getCostFactor(job1: JobExecution, job2: JobExecution, iteration: Integer, adjustedRuntimes: ListBuffer[Double]): Double = {
    val costFactor = job1.iterations.apply(iteration).runtime / adjustedRuntimes(iteration)
    if (java.lang.Double.isNaN(costFactor)) {
      1
    } else {
      costFactor
    }
  }
}