package de.tuberlin.cit.progressestimator.scaleout

import de.tuberlin.cit.allocationassistant.AllocationAssistant
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.entity.JobExecution
import com.google.inject.Inject
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.allocationassistant.PreviousRuns
import scala.collection.mutable.HashMap
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.File
import com.google.inject.Singleton
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig

/**
 * Wrapper for the Bell adapter which caches the scaleout models to disk for performance improvements.
 */
@Singleton
class CachedBellAdapter(override val jobHistory: JobHistory, override val config: EstimatorConfig) extends BellAdapter(jobHistory, config) {

  private val diskFile = config.directoryCaches + "//bell-cache-extended-" + jobHistory.getHistoryId()
  //                        jobname        dataset       iteration        inputsize   Model
  var cache = HashMap.empty[String, HashMap[String, HashMap[Integer, HashMap[Double, Array[(Int, Double)]]]]]
  readFromDisk()
  var putCounter = 0

  /**
   * returns the predicted runtime of the given iteration of the given job with the given input size,
   * when executed on the given number of nodes.
   */
  override def getRuntimePrediction(job: JobExecution, inputSize: Double, numberOfNodes: Int, currentIteration: Int): Double = {

    val modelCache = getCache(job, inputSize, currentIteration)

    if (!modelCache.contains(inputSize)) {
      val model = super.getModel(job, inputSize, currentIteration)
      modelCache.put(inputSize, model)
      if (putCounter > 30) {
        putCounter = 0
        saveToDisk()
      }
      putCounter += 1
    } else {
    }
    val model = modelCache.get(inputSize).get

    return model.filter(_._1 == numberOfNodes)(0)._2

  }

  /**
   * helper retrieving the appropriate cache instance.
   * if any of the caches does not exist, it is created.
   */
  private def getCache(job: JobExecution, inputSize: Double, currentIteration: Int): HashMap[Double, Array[(Int, Double)]] = {
    val round = Math.pow(10, Math.log10(inputSize).toInt - 1)

    val iterationKey = getIterationKey(currentIteration)
    val iterationCache = getIterationCache(job)

    if (!iterationCache.contains(iterationKey)) {
      val empty = HashMap.empty[Double, Array[(Int, Double)]]
      iterationCache.put(iterationKey, empty)
    }
    iterationCache.get(iterationKey).get
  }
  
  /**
   * helper retrieving the appropriate iteration cache instance.
   * if any of the caches does not exist, it is created.
   */
  private def getIterationCache(job: JobExecution) : HashMap[Integer, HashMap[Double, Array[(Int, Double)]]]= {
    
    val datasetKey = getDatasetKey(job)
    if (!cache.contains(job.name)) {
      val empty = HashMap.empty[String, HashMap[Integer, HashMap[Double, Array[(Int, Double)]]]]
      cache.put(job.name, empty)
    }
    val datasetCache = cache.get(job.name).get

    if (!datasetCache.contains(datasetKey)) {
      val empty = HashMap.empty[Integer, HashMap[Double, Array[(Int, Double)]]]
      datasetCache.put(datasetKey, empty)
    }
    val iterationCache = datasetCache.get(datasetKey).get
    iterationCache
  }
  /**
   * returns the cache key of the given iteration
   */
  private def getIterationKey(currentIteration : Int): Int = {
    if (config.trainingDataSameIterationOnly) {
      currentIteration
    } else {
      -1
    }
  }
  /**
   * returns the dataset key of the given iteration
   */
  private def getDatasetKey(job: JobExecution): String = {
    if (config.trainingDataSameDatasetOnly) {
      job.input.name
    } else {
      "ALL-DATASETS"
    }
  }
  
  /**
   * read cache from disk
   */
  def readFromDisk(): Unit = {

    if (new File(diskFile).exists) {
      val fis = new FileInputStream(diskFile)
      val ois = new ObjectInputStream(fis)
      cache = ois.readObject.asInstanceOf[HashMap[String, HashMap[String, HashMap[Integer, HashMap[Double, Array[(Int, Double)]]]]]]

      println(s"Read Bell Cache from disk. Size=${cache.size}")
      ois.close()
    }
  }
  /**
   * save cache to disk
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