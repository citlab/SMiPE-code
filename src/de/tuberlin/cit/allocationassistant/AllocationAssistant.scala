package de.tuberlin.cit.allocationassistant

import de.tuberlin.cit.allocationassistant.prediction.ScaleOutPredictor

case class PreviousRuns(scaleOuts: Array[Integer], runtimes: Array[Double], datasetSizes: Array[Double])

object AllocationAssistant {

    def computeScaleOutModel(scaleOuts: Array[Int], runtimes: Array[Double], constraint: (Int, Int), target: Double): Array[(Int, Double)] = {

    // argsort scale-outs
    val idxs = scaleOuts.zipWithIndex.sortBy(_._1).map(_._2)

    // sort scale-outs and runtimes array
    val scaleOutsSorted = idxs.map(scaleOuts(_))
    val runtimesSorted = idxs.map(runtimes(_))

    val (minScaleOut, maxScaleOut) = constraint

    // if there are at least 3 scale-outs use our model
    val predictor = new ScaleOutPredictor
    val result = predictor.computeScaleOutModel(scaleOutsSorted, runtimesSorted, constraint, target)

    result
  }

  def filterPreviousRuns(previousRuns: PreviousRuns, targetDatasetSize: Double): (Array[Int], Array[Double]) = {

    val t = (previousRuns.scaleOuts zip previousRuns.runtimes zip previousRuns.datasetSizes)
      .filter({
        case ((_, _), datasetSize) => .9 * targetDatasetSize < datasetSize && datasetSize < 1.1 * targetDatasetSize
      })
      .map({
        case ((scaleOut, runtime), _) => (scaleOut.toInt, runtime.toDouble)
      })
      .unzip
     (t._1.toArray, t._2.toArray)
  }


}
