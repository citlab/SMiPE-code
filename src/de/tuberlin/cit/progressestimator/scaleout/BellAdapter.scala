package de.tuberlin.cit.progressestimator.scaleout

import de.tuberlin.cit.allocationassistant.AllocationAssistant
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.entity.JobExecution
import com.google.inject.Inject
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.allocationassistant.PreviousRuns
import com.google.inject.Singleton
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig

/**
 * This is an adapter class for the Bell Model.
 * This class uses the allocation assistant to compute a Scale Out Model.
 * 
 * The allocation assistant is the implementation of the BELL model
 * (Paper "Continuously Improving the Resource Utilization of Iterative Parallel Dataflows" by
 * Lauritz Thamsen, Thomas Renner and  Odej Kao (TU Berlin)
 * Distributed Computing Systems Workshops (ICDCSW)
 * 
 */
@Singleton
class BellAdapter(val jobHistory: JobHistory, val config: EstimatorConfig) extends ScaleOutModel {
  

  /**
   * returns the scale-out model for the given job, input size and iteration.
   */
  protected def getModel(job: JobExecution, inputSize : Double, iteration : Int) : Array[(Int, Double)] = {
    
    val previousRuns = getTrainingDataIndividualIterations(job, iteration)  
    val (scaleOuts, runtimes) = AllocationAssistant.filterPreviousRuns(previousRuns, inputSize)
    val numPrevRuns = scaleOuts.length

    val maxRuntime = 0.0
    val scaleOutModel = AllocationAssistant.computeScaleOutModel(scaleOuts, runtimes, config.bellScaleoutConstraint, maxRuntime)
    if(scaleOutModel(0)._2 == .0 && scaleOutModel(5)._2 == .0 && scaleOutModel(10)._2 == .0) {
      val c = 0
    }
    scaleOutModel
  }
    
  /**
   * returns the predicted runtime of the given iteration of the given job with the given input size, 
   * when executed on the given number of nodes.
   */
  override def getRuntimePrediction(job: JobExecution, inputSize : Double, numberOfNodes : Int, currentIteration : Int): Double = {
    val scaleOutModel = getModel(job, inputSize, currentIteration)
    scaleOutModel.filter(_._1 == numberOfNodes)(0)._2
    
  }
  
  
  /**
   * returns the training data to use for the scale out model
   */
  private def getTrainingDataIndividualIterations(job: JobExecution, iteration : Integer): PreviousRuns = {
    val jobs = if(config.trainingDataSameDatasetOnly) { 
      jobHistory.getJobExecutions(job.name).filter(_.input.name.equals(job.input.name))
    } else {
      jobHistory.getJobExecutions(job.name)
    }
    val scaleOuts: ListBuffer[Integer] = new ListBuffer[Integer]()
    val runtimes: ListBuffer[Double] = new ListBuffer[Double]()
    val datasetSizes: ListBuffer[Double] = new ListBuffer[Double]()
    for (j <- jobs) { 
      if(config.trainingDataSameIterationOnly) {
        if(j.iterations.length > iteration) {
          val i = j.iterations.apply(iteration)
          scaleOuts += i.resourceConfig.numberOfNodes
          runtimes += i.runtime
          datasetSizes += i.input.numberOfRecords
        }
      } else {
        for (i <- j.iterations) {
          scaleOuts += i.resourceConfig.numberOfNodes
          runtimes += i.runtime
          datasetSizes += i.input.numberOfRecords
        }
      }
    }

    PreviousRuns(scaleOuts.toArray, runtimes.toArray, datasetSizes.toArray)
  }

}