package de.tuberlin.cit.progressestimator.scaleout

import de.tuberlin.cit.progressestimator.entity.JobExecution

/**
 * This represents a Scale-Out model used to adjust runtimes for executions
 * with different scale-outs
 */
trait ScaleOutModel {
  /**
   * returns the predicted runtime of the given iteration of the given job with the given input size, 
   * when executed on the given number of nodes.
   */
  def getRuntimePrediction(job: JobExecution, inputSize : Double, numberOfNodes : Int, currentIteration : Int): Double
}