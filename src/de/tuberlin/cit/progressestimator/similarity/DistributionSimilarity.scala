package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.{ Iteration, JobExecution }
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.util.WeightFunctions

/** 
 *  This class is a generic similarity for Distributions.
 *  It is used for similarities comparing pairwise values such as the Runtime Similarity
 *  or the Active Data REcords similarity.
 */
class DistributionSimilarity(val simResponse: SimulationResponse, iterationProperty: ((Iteration, ListBuffer[Double])) => Double, normalize: Boolean = true, simName: String = "DistributionSimilarity") extends Similarity {

  var weightFunction = WeightFunctions.weightFunction1
  var weightFunctionId = "weightFunction1"
  KEEP_THRESHOLD = .8
  /**
   * returns the unique id
   */
  def getIdentifier() : String = {
    "DistributionSimilarity," + weightFunctionId + "," + deviationFunctionId
  }

  
  /**
   * returns the similarity
   */
  def getSimilarity(job1: JobExecution, 
                    job2: JobExecution,
                    currentIteration: Int, 
                    job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()
                    ): (Boolean, Double) = {

    var dataPoints1: ListBuffer[Double] = null
    var dataPoints2: ListBuffer[Double] = null
    var deviations: List[Double] = null
    if (normalize) {
      dataPoints1 = getNormalizedPoints(job1, currentIteration, null)
      dataPoints2 = getNormalizedPoints(job2, currentIteration, job2AdjustedRuntimes)
      deviations = List.range(0, currentIteration - 1).map(i => deviationFunction(dataPoints1(i), dataPoints2(i)))
    } else {
      dataPoints1 = job1.iterations.take(currentIteration).map(iterationProperty(_, null))
      dataPoints2 = job2.iterations.take(currentIteration).map(iterationProperty(_, job2AdjustedRuntimes))
      deviations = List.range(0, currentIteration - 1).map(i => deviationFunction(dataPoints1(i), dataPoints2(i)))
    }

    simResponse.addDiagram(job2, dataPoints1, dataPoints2, simName + (if (normalize) " normalized" else ""))

    val avgDeviation = getAvg(deviations)
    if(avgDeviation < 0) {
      val y =1
    }
    var similarity = avgDeviation

    val eventTitle = simName + (if (normalize) " normalized" else "")
    val eventValue = "%.4f".format(similarity)
    simResponse.addSimilarityLog(job2, eventTitle, eventValue)
    (similarity > KEEP_THRESHOLD, similarity)
  }
  /**
   * sets the weight function
   */
  def setWeightFunction(f: (Int, Int) => Double, weightFunctionId : String) : DistributionSimilarity = {
    weightFunction = f
    this.weightFunctionId = weightFunctionId
    this
  }
  
  /**
   * returns the average.
   */
  def getAvg(deviations : List[Double]) : Double = {
    val weights = List.range(0, (deviations.length)).map(weightFunction(deviations.length, _))
    
    val weightedDeviationsSum = deviations.zip(weights).map(t => t._1 * t._2).sum
    val weightsSum = weights.sum
    weightedDeviationsSum / weightsSum
  }

  /**
   * returns the normalized points, i.e. relative to the first iteration.
   */
  def getNormalizedPoints(job: JobExecution, currentIteration: Integer, job2AdjustedRuntimes: ListBuffer[Double]): ListBuffer[Double] = {
    var firstValue = iterationProperty(job.iterations.apply(0), job2AdjustedRuntimes)
    if (firstValue == 0) {
      println("no first value: " + job.iterations.apply(0).input.numberOfRecords)
      return new ListBuffer[Double]
    } else {
      return job.iterations.take(currentIteration).map[Double, ListBuffer[Double]](i => iterationProperty(i, job2AdjustedRuntimes) / firstValue)
    }
  }
  
    
  private var deviationFunction = (val1 : Double, val2 : Double) => Math.min(val1, val2) / Math.max(val1, val2)
  private var deviationFunctionId = "standard-deviation"
  
  /**
   * call this function to use the delta deviation method.
   */
  def useDeltaDeviation() : Similarity = {
    deviationFunction = (val1 : Double, val2 : Double) => {
      val delta = Math.abs(val1 - val2)
      if(val1 > 0) {
        1  - (delta / val1)
      } else {
        .0
      } 
    }
    deviationFunctionId = "delta-deviation"
    this
  }
  
  
}