package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import scala.collection.mutable.HashMap
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.time.Instant
import com.google.inject.Inject

/**
 * This class determines the similarity of two job executions according
 * to their input dataset sizes.
 *
 */
class DstatSimilarity(val simResponse: SimulationResponse,
                      val property: String = "idl",val dstatCache: DstatCache) extends Similarity {
  KEEP_THRESHOLD = .5

  def getIdentifier(): String = {
    "DstatSimilarity," + dstatValueId + "," + deviationFunctionId
  }

  var dstatValueId = property
  var dstatValue: ((String) => Double, (String, String, String) => Double) => Double = (a, b) => a(property)

  def setDstatValue(f: ((String) => Double, (String, String, String) => Double) => Double, id: String): DstatSimilarity = {
    dstatValue = f
    dstatValueId = id
    this
  }

  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {

    var timeEndJob1 = job1.iterations.apply(currentIteration).timeFinish
    var timeEndJob2 = job2.iterations.apply(currentIteration).timeFinish
    var timeStartJob1 = job1.iterations.apply(0).timeStart
    var timeStartJob2 = job2.iterations.apply(0).timeStart

    val retrieveDstatValueFunction1 = (prop: String) => dstatCache.getValueFromCache(prop, job1, currentIteration, timeStartJob1, timeEndJob1)
    val retrieveDstatValueFromExpressionFunction1 =
      (prop1: String, prop2: String, operator: String) => dstatCache.getValueFromCache(job1, currentIteration, timeStartJob1, timeEndJob1, prop1, prop2, operator)
    val retrieveDstatValueFunction2 = (prop: String) => dstatCache.getValueFromCache(prop, job2, currentIteration, timeStartJob2, timeEndJob2)
    val retrieveDstatValueFromExpressionFunction2 =
      (prop1: String, prop2: String, operator: String) => dstatCache.getValueFromCache(job2, currentIteration, timeStartJob2, timeEndJob2, prop1, prop2, operator)

    val job1Value = dstatValue(retrieveDstatValueFunction1, retrieveDstatValueFromExpressionFunction1)
    val job2Value = dstatValue(retrieveDstatValueFunction2, retrieveDstatValueFromExpressionFunction2)

    var smaller = Math.min(job1Value, job2Value)
    var bigger = Math.max(job1Value, job2Value)

    var keep = true
    var similarity = .0
    if (smaller == Double.NaN || bigger == Double.NaN) {
      simResponse.addJobLog(job2, "One Dstat value is NaN")
    } else if (smaller == 0 || bigger == 0) {
      simResponse.addJobLog(job2, "One Dstat value is 0")
    } else {
      similarity = smaller / bigger
      keep = similarity > KEEP_THRESHOLD
    }
    if (useDeltaDeviationFlag) {
      val delta = Math.abs(job1Value - job2Value)
      if (job1Value > 0) {
        similarity = 1 - (delta / job1Value)
      } else {
        keep = true
        similarity = 0
      }
    }
    simResponse.addSimilarityLog(job2, s"DstatSimilarity [$property] Similarity", "" + similarity)
    return (keep, similarity)
  }

  private var useDeltaDeviationFlag = false
  private var deviationFunctionId = "standard-deviation"
  def useDeltaDeviation(): DstatSimilarity = {
    useDeltaDeviationFlag = true
    deviationFunctionId = "delta-deviation"
    this
  }
}