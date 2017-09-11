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
import com.google.inject.Singleton
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import com.google.inject.Inject

/**
 * Cache for Dstat values for performance improvements
 * 
 * The class uses four caches.
 * cacheAllNodes: Caches the average values for  all nodes
 * cache: Caches the average of the average values for all nodes
 * cacheStart: caches the values, but begins when the first iteration starts. This is the preferred version
 * cacheComplex: caches combination of values, e.g. multiplications and division.
 */
@Singleton
class DstatCache @Inject() (val config: EstimatorConfig) {
  
  
  //                    Property          jobid       iteration     value
  var cacheAllNodes = HashMap.empty[String, HashMap[Int, HashMap[Int, Array[Double]]]]
  var cache = HashMap.empty[String, HashMap[Int, HashMap[Int, Double]]]
  var cacheStart = HashMap.empty[String, HashMap[Int, HashMap[Int, Double]]]
  var cacheComplex = HashMap.empty[String, HashMap[Int, HashMap[Int, Double]]]
  var counter = 0
  
  readFromDisk()
  
  def getValueOfAllNodesFromCache(property:String,job: JobExecution, iteration: Int, timeStart: Instant, timeEnd: Instant): Array[Double] = {
    val usedCache = cacheAllNodes
    val key = property
    if(!usedCache.contains(key)) {
      usedCache.put(key, HashMap.empty[Int, HashMap[Int, Array[Double]]])
    }
    val cache = usedCache.get(key).get
    if (!cache.contains(job.id)) {
      cache.put(job.id, HashMap.empty[Int, Array[Double]])
    }
    val jobMap = cache.get(job.id).get
    if (jobMap.contains(iteration)) {
      jobMap.get(iteration).get
    } else {
      val value = job.dstatAverage(property, timeStart, timeEnd).map(_._2).toArray
      if (counter > 150) {
        saveToDisk()
        counter = 0
      }
      jobMap.put(iteration, value)
      counter += 1
      value
    }
  }

  def getValueFromCache(property:String, job: JobExecution, iteration: Int, time: Instant): Double = {
    val key = property
    val usedCache = this.cache
    val valueFunction = () => {
      val hostAvg = job.dstatAverage(property, time).map(_._2)
      val value = hostAvg.sum / hostAvg.length
      value
    }
    getValueFromCacheHelper(key, usedCache, valueFunction, job, iteration)

  }
  def getValueFromCache(property:String,job: JobExecution, iteration: Int, timeStart: Instant, timeEnd: Instant): Double = {
    val key = property
    val usedCache = this.cacheStart
    val valueFunction = () => {
      if(property.equals("idl-nodes")) {
        val hostAvg = job.dstatAverage("idl", timeStart, timeEnd).map(_._2)
        val value = hostAvg.filter(_ > .95).length
        value
      } else {
        val hostAvg = job.dstatAverage(property, timeStart, timeEnd).map(_._2)
        val value = hostAvg.sum / hostAvg.length
        value
      }
    }
    getValueFromCacheHelper(key, usedCache, valueFunction, job, iteration)
    
  }
  def getValueFromCache(job: JobExecution, iteration: Int, timeStart: Instant, timeEnd: Instant, property1:String, property2:String,operator:String): Double = {
    val key = property1 + operator + property2
    val usedCache = this.cacheComplex
    val valueFunction = () => {
      val hostAvg = job.dstatAverage(property1, property2, operator, timeStart, timeEnd).map(_._2)
      val value = hostAvg.sum / hostAvg.length
      value
    }
    getValueFromCacheHelper(key, usedCache, valueFunction, job, iteration)
    
  }
  
  
  private def getValueFromCacheHelper(key : String, usedCache : HashMap[String, HashMap[Int, HashMap[Int, Double]]], 
      valueFunction : () => Double,
      job: JobExecution, iteration: Int): Double = {
    if(!usedCache.contains(key)) {
      usedCache.put(key, HashMap.empty[Int, HashMap[Int, Double]])
    }
    val cache = usedCache.get(key).get
    if (!cache.contains(job.id)) {
      cache.put(job.id, HashMap.empty[Int, Double])
    }
    val jobMap = cache.get(job.id).get
    if (jobMap.contains(iteration)) {
      jobMap.get(iteration).get
    } else {
      val value = valueFunction()
      if (counter > 150) {
        saveToDisk()
        counter = 0
      }
      jobMap.put(iteration, value)
      counter += 1
      value
    }
  }

  private def diskFile = config.directoryCaches + "//dstat-avg-cache"
  def readFromDisk(): Unit = {

    if (new File(diskFile).exists) {
      val fis = new FileInputStream(diskFile)
      val ois = new ObjectInputStream(fis)
      cache = ois.readObject.asInstanceOf[HashMap[String, HashMap[Int, HashMap[Int, Double]]]]
      ois.close()
    }

    if (new File(diskFile  + "-start").exists) {
      val fis1 = new FileInputStream(diskFile  + "-start")
      val ois1 = new ObjectInputStream(fis1)
      cacheStart = ois1.readObject.asInstanceOf[HashMap[String, HashMap[Int, HashMap[Int, Double]]]]
      ois1.close()
    }
    if (new File(diskFile  + "-complex").exists) {
      val fis1 = new FileInputStream(diskFile  + "-complex")
      val ois1 = new ObjectInputStream(fis1)
      cacheComplex = ois1.readObject.asInstanceOf[HashMap[String, HashMap[Int, HashMap[Int, Double]]]]
      ois1.close()
    }
    if (new File(diskFile  + "-allnodes").exists) {
      val fis1 = new FileInputStream(diskFile  + "-allnodes")
      val ois1 = new ObjectInputStream(fis1)
      cacheAllNodes = ois1.readObject.asInstanceOf[HashMap[String, HashMap[Int, HashMap[Int, Array[Double]]]]]
      ois1.close()
    }
    
    println(s"Read Dstat Avg Cache  from disk. Size=${cache.size}")
  }
  def saveToDisk() = {
    try {
      val fos = new FileOutputStream(diskFile)
      val oos = new ObjectOutputStream(fos)
      oos.writeObject(this.cache)
      oos.close()

      val fos1 = new FileOutputStream(diskFile + "-start")
      val oos1 = new ObjectOutputStream(fos1)
      oos1.writeObject(this.cacheStart)
      oos1.close()
      
      val fos2 = new FileOutputStream(diskFile + "-complex")
      val oos2 = new ObjectOutputStream(fos2)
      oos2.writeObject(this.cacheComplex)
      oos2.close()
      
      val fos3 = new FileOutputStream(diskFile + "-allnodes")
      val oos3 = new ObjectOutputStream(fos3)
      oos3.writeObject(this.cacheAllNodes)
      oos3.close()

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}