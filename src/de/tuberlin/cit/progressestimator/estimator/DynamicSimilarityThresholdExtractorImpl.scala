package de.tuberlin.cit.progressestimator.estimator

import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import scala.collection.mutable.HashMap
import de.tuberlin.cit.progressestimator.similarity.Similarity
import com.google.inject.Inject
import de.tuberlin.cit.progressestimator.history.JobHistory
/**
 * This class extract trained values for the similarity thresholds.
 * It stores and retrieves similarity quality data which is used to find the 
 * thresholds.
 * 
 */
class DynamicSimilarityThresholdExtractorImpl @Inject() (val jobHistory: JobHistory, val config: EstimatorConfig) extends DynamicSimilarityThresholdExtractor {

  var cache = HashMap.empty[String, (Array[(Double, Double)], Array[(Double, Double)])]
  readFromDisk()

  val minimumShare = config.dynamicThresholdsMinimumJobShare
  val minimumAccuracy = config.dynamicThresholdsMinimumAccuracy
  
  /**
   * returns the trained threshold for the given job and similarity.
   */
  def getThreshold(jobName: String, sim: Similarity): Double = {
    val histogram = getHistogram(jobName, sim)
    val count = getCount(jobName, sim)
    if (histogram == null || count == null) {
      return Double.NaN
    }

    if (histogram != null && histogram.size >= 99) {
      var nextThreshold = .90
      var threshold = nextThreshold
      var constraintsMet = false
      
      // loop through threshold from 1 to 0 until threshold meeting the constraints was found
      do {
        threshold = nextThreshold
        nextThreshold -= .01
        val nextAccuracy = getHistogramValue(histogram, nextThreshold)
        val nextShare = getHistogramValue(count, nextThreshold)
        constraintsMet = nextAccuracy < minimumAccuracy && nextShare >= minimumShare
      } while (threshold > 0.01 && !constraintsMet)

      threshold
    } else {
      Double.NaN
    }
  }

  /**
   * adds the given histogram / point values to the cache
   */
  def addHistogramToCache(jobName: String, sim: Similarity, histogram: Array[(Double, Double)], count: Array[(Double, Double)]) = {
    cache.put(getCacheKey(jobName, sim), (histogram, count))
    saveToDisk
  }

  /**
   * helper returning the value with the given threshold from the given histogram
   */
  private def getHistogramValue(histogram: Array[(Double, Double)], threshold: Double): Double = {
    val p = histogram.apply((threshold * 100).toInt)
    p._2
  }
  /**
   * retrieves the histogram from the cache
   */
  private def getHistogram(jobName: String, sim: Similarity): Array[(Double, Double)] = {
    val key = getCacheKey(jobName, sim)
    if (cache.contains(key)) {
      cache.get(key).get._1
    } else {
      null
    }
  }
  /**
   * retrieves the point shares from the cache
   */
  private def getCount(jobName: String, sim: Similarity): Array[(Double, Double)] = {
    val key = getCacheKey(jobName, sim)
    if (cache.contains(key)) {
      val countM = cache.get(key).get._2

      if (countM.length > 50 && countM(5)._2 < countM(40)._2) {
        countM.reverse
      } else {
        countM
      }
    } else {
      null
    }
  }

  /**
   * returns the cache key for the given job and similarity
   */
  private def getCacheKey(jobName: String, sim: Similarity): String = {
    return jobName + "|" + sim.getIdentifier()
  }

  private def diskFile = config.directoryCaches + "//sim-histogram-count-cache" + jobHistory.getHistoryId()
  /**
   * reads the cache from disk
   */
  def readFromDisk(): Unit = {

    if (new File(diskFile).exists) {
      val fis = new FileInputStream(diskFile)
      val ois = new ObjectInputStream(fis)
      cache = ois.readObject.asInstanceOf[HashMap[String, (Array[(Double, Double)], Array[(Double, Double)])]]

      ois.close()
    }
  }
  /**
   * saves the cache to disk
   */
  def saveToDisk() = {
    try {
      val fos = new FileOutputStream(diskFile)

      val oos = new ObjectOutputStream(fos)
      oos.writeObject(this.cache);
      oos.close();

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}