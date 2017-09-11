package de.tuberlin.cit.progressestimator.estimator;

import scala.collection.mutable.ListBuffer
import org.jblas.Solve
import org.jblas.DoubleMatrix
import edu.rit.numeric.NonNegativeLeastSquares
import edu.rit.numeric.LinearSolve
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresFactory
import org.apache.commons.math3.linear.Array2DRowRealMatrix
import org.apache.commons.math3.linear.LUDecomposition
import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.commons.math3.linear.QRDecomposition
import org.apache.commons.math3.linear.SingularValueDecomposition
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.stat.regression.GLSMultipleLinearRegression
import weka.core.Instances
import org.apache.commons.math3.optimization.linear.SimplexSolver
import java.util.Collection
import org.apache.commons.math3.optimization.linear.LinearConstraint
import java.util.ArrayList
import org.apache.commons.math3.optimization.linear.Relationship
import org.apache.commons.math3.optimization.GoalType
import org.apache.commons.math3.optimization.linear.LinearObjectiveFunction
import org.apache.commons.math3.analysis.MultivariateFunction
import org.apache.commons.math3.optimization.direct.PowellOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction
import org.apache.commons.math3.optimization.InitialGuess
import org.apache.commons.math3.optimization.direct.NelderMeadSimplex
import org.apache.commons.math3.optimization.direct.SimplexOptimizer
import org.apache.commons.math3.optimization.direct.BOBYQAOptimizer
import scala.collection.mutable.HashMap
import com.google.inject.Inject
import de.tuberlin.cit.progressestimator.history.JobHistory
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import de.tuberlin.cit.progressestimator.similarity.WeightedSimilarityCombination

case class PowellJobData(val jobVectors: Array[(Array[Double], Double)], val runtimeActual: Double, val job: String, val input: String)

/**
 * This class performs the optimization for finding optimal similarity weights.
 * Information from job simulations are added using the addJobInformation function.
 * After all information of a simulation suite have been added, the optimization
 * is performed using solve().
 * The class also stores and retrieves trained weights to and from a cache.
 */
class SimilarityWeightsSolver @Inject() (val jobHistory: JobHistory, val config: EstimatorConfig) {

  //                             alog+dataset       sim    weight
  var weightsCache = HashMap.empty[String, HashMap[String, Double]]
  readFromDisk

  /**
   * solves the similarity weights optimization problem.
   * If saveWeightsCache is true, the results are stored to the cache
   * so they can be used by the estimator.
   */
  def solve(saveWeightsCache: Boolean) = {
    try {
      solveBOBYQA(saveWeightsCache)
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  /**
   * The similarity ids of the trained similarities.
   * This information is needed to map a value in the JobData vector to a certain similarity.
   */
  var simIds: ListBuffer[String] = null

  /**
   * The vectors needed for an efficient calculation of
   * the objective function.
   */
  val jobData = new ListBuffer[PowellJobData]()
  def addJobData(v: PowellJobData) = {
    jobData += v
  }

  /**
   * The objective function used for the optimization.
   * It returns the mean relative error for the simulations represented
   * by the previously added vector values.
   * The vector values contain the information necessary to calculate the
   * mean relative error. This method is more efficient than calling the acutal
   * estimator in the objective function.
   */
  class PowellObjectiveFunction(val useAbs: Boolean = false, val addPenaltyForNegativeWeights: Boolean = false, filterJob: String = null, filterDataset: String = null) extends MultivariateFunction {
    def value(x1: Array[Double]): Double = {
      
      val keepThreshold = (new WeightedSimilarityCombination(null, 1)).KEEP_THRESHOLD
      val x = if (useAbs) {
        x1.map(Math.abs(_))
      } else {
        x1
      }

      val filteredData = jobData.filter(t => {
        (filterJob == null || t.job.equals(filterJob)) && (filterDataset == null || t.input.equals(filterDataset)) && (t.jobVectors.length > 0)
      })
      val simulationErrors = filteredData.map(relativeError(_, x, keepThreshold))
      var meanError = simulationErrors.sum / simulationErrors.length
      if (addPenaltyForNegativeWeights) {
        x1.foreach(v => if (v < 0) meanError += v * -1 * 10000)
      }
      meanError
    }
    
    private def relativeError(t : PowellJobData, x: Array[Double], keepThreshold : Double) : Double = {
       val vectorsWithSimilarity = t.jobVectors.map(j => {
          val jobSimilarities = j._1
          val prediction = j._2
          val totalSimilarity = jobSimilarities.zip(x).map(t => t._1 * t._2).sum / x.sum
          (jobSimilarities, prediction, totalSimilarity)
        })

        // add filters and transformation function
        val filteredVectors = vectorsWithSimilarity.filter(j => j._3 > keepThreshold).sortBy(_._3).reverse.take(config.maxSimilarJobs).map(j => {
          val sim = Math.pow(j._3, config.finalSimilarityExponent)
          (j._1, j._2, sim)
        })
        val finalVectors = if (filteredVectors.length > 0) {
          filteredVectors
        } else {
          vectorsWithSimilarity.sortBy(_._3).reverse.take(5)
        }

        var predTop = .0
        var predBottom = .0
        filteredVectors.foreach(j => {
          val jobRemaining = j._2
          val totalSim = j._3
          predTop += totalSim * jobRemaining
          predBottom += totalSim
        })
        val estimate = predTop / predBottom

        val error = Math.abs(estimate - t.runtimeActual) / t.runtimeActual
        error
    }
  }
  private def resetWeightsCache = weightsCache = HashMap.empty[String, HashMap[String, Double]]

  /**
   * helper creating guess values for the BOBYQA
   */
  private def createGuesses(solutionDimension: Int): ListBuffer[Array[Double]] = {

    val guess = List.range(0, solutionDimension - 1).map(_ => 1.0).toArray
    val guesses = ListBuffer(guess)
    for (i <- List.range(0, solutionDimension - 1)) {
      val a = guess.map(_ => 0.0)
      a(i) = 1.0
      guesses += a
    }
    guesses
  }
  /**
   * performs the BOBYQA optimization
   */
  def solveBOBYQA(saveWeightsCache: Boolean) = {
    println("### Weights optimization ")

    if (saveWeightsCache) {
      resetWeightsCache
    }

    val optimizer = new BOBYQAOptimizer(simIds.length + 2)
    val solutionDimension = simIds.size
    val guesses = createGuesses(solutionDimension)

    val algorithms = jobData.groupBy(_.job)
    algorithms.foreach(t => {
      val algo = t._1
      powellFindBestSolution(optimizer, guesses, algo, null)
    })

    if (saveWeightsCache) {
      saveToDisk()
    }

  }

  /**
   * performs the optimization for the given guesses and finds the best solution.
   * This solution is put to the cache.
   */
  def powellFindBestSolution(optimizer: BOBYQAOptimizer, guesses: ListBuffer[Array[Double]], algo: String, dataset: String) = {

    val solutions: ListBuffer[(String, Double, String, Array[Double])] = ListBuffer()
    powellFindBestSolutionForAllGuesses(solutions, optimizer, guesses, algo, dataset)

    val bestSolutions = printBestSolutions(solutions, algo, dataset)
    putBestSolutionsToCache(bestSolutions, algo, dataset)

  }
  /**
   * helper which puts the best solution to the cache
   */
  private def putBestSolutionsToCache(bestSolutions: ListBuffer[(String, Double, String, Array[Double])], algo: String, dataset: String) = {
    val bestSolution = bestSolutions.apply(0)
    if (!bestSolution._2.equals(Double.NaN)) {

      val weights = bestSolution._4
      simIds.zip(weights).foreach(t => {
        val weight = t._2
        val simId = t._1
        putWeightToCache(algo, dataset, simId, weight)
      })

    }
  }
  /**
   * helper which prints the best solution
   *
   */
  private def printBestSolutions(solutions: ListBuffer[(String, Double, String, Array[Double])],
                                 algo: String, dataset: String): ListBuffer[(String, Double, String, Array[Double])] = {
    val bestSolutions = solutions.sortBy(_._2).take(1)
    println(s"Optimal solution: $algo " + (if(dataset == null) "" else dataset))
    bestSolutions.foreach(s => println(s._3))
    bestSolutions
  }

  private def powellFindBestSolutionForAllGuesses(solutions: ListBuffer[(String, Double, String, Array[Double])], optimizer: BOBYQAOptimizer, guesses: ListBuffer[Array[Double]], algo: String, dataset: String) = {
    guesses.foreach(g => {
      val gs = "  {" + g.mkString(" | ") + "}"

      // try two different objective functions which avoid negative solutions in different ways
      // the first adds a penalty if the solution contains a negative value
      // the second uses the absolute value of every value in the solution.
      val fPenalty = new PowellObjectiveFunction(false, true, filterJob = algo, filterDataset = dataset)
      val fAbs = new PowellObjectiveFunction(true, false, filterJob = algo, filterDataset = dataset)

      solutions += powellHelper(s"BOBYQA penalty $algo $dataset $gs", optimizer, fPenalty, g, algo, dataset)
      solutions += powellHelper(s"BOBYQA abs $algo $dataset $gs", optimizer, fAbs, g, algo, dataset)
    })
  }
  /**
   * runs the Powell Optimization, normalizes the solution and returns  a vector
   * (name, avgPercent, solutionStringRepresntation, normalizedSolution)
   */
  private def powellHelper(name: String, optimizer: BOBYQAOptimizer, f: MultivariateFunction, guess: Array[Double], algo: String, dataset: String): (String, Double, String, Array[Double]) = {

    val result = optimizer.optimize(1000000, f, GoalType.MINIMIZE, guess)
    val solution = result.getPoint.map(Math.abs(_))

    val sumWeights = solution.sum
    val vectorNormalized = solution.map(s => (s / sumWeights * 1000).toInt.toDouble / 10)

    val dev = evaluateSolution(vectorNormalized, algo, dataset)

    (name, dev._1, dev._2, vectorNormalized)
  }

  /**
   * returns the mean relative error according to the objective function together with a string
   * which contains the solution and the error.
   */
  private def evaluateSolution(x: Array[Double], algo: String = null, dataset: String = null): (Double, String) = {
    val f = new PowellObjectiveFunction(true, false, filterJob = algo, filterDataset = dataset)

    val avgPercent = f.value(x)
    (avgPercent, "# [" + x.map("%.4f".format(_)).mkString(" | ") + "]   AVG dev " + "%.3f".format(avgPercent * 100) + "%")
  }

  /**
   * puts the given weight for the similarity with the given id to the cache.
   */
  private def putWeightToCache(algo: String, dataset: String, simId: String, weight: Double) = {
    val key = s"$algo-$dataset"
    if (!weightsCache.contains(key)) {
      val c = HashMap.empty[String, Double]
      weightsCache.put(key, c)
    }
    val jobCache = weightsCache.get(key).get
    jobCache.put(simId, weight)
  }
  /**
   * retrieves the weight for the similarity with the given id from the cache
   */
  def getFromCache(algo: String, dataset: String, simId: String): Double = {
    val key = s"$algo-$dataset"
    if (weightsCache.contains(key)) {
      val jobCache = weightsCache.get(key).get
      val w = jobCache.get(simId)
      if (w.isDefined) {
        w.get
      } else {
        -1.0
      }
    } else {
      -1.0
    }
  }
  private def diskFile = config.directoryCaches + "//sim-weights-cache" + jobHistory.getHistoryId()
  def readFromDisk(): Unit = {

    if (new File(diskFile).exists) {
      val fis = new FileInputStream(diskFile)
      val ois = new ObjectInputStream(fis)
      weightsCache = ois.readObject.asInstanceOf[HashMap[String, HashMap[String, Double]]]

      ois.close()
    }
  }
  def saveToDisk() = {
    try {
      val fos = new FileOutputStream(diskFile)

      val oos = new ObjectOutputStream(fos)
      oos.writeObject(weightsCache)
      oos.close()

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
