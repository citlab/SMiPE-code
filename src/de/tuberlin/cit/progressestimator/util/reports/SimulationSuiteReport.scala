package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import scala.collection.mutable.HashMap
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import de.tuberlin.cit.progressestimator.estimator.SimilarityWeightsSolver
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.exception.MathIllegalArgumentException

/**
 * Create an html report with information on the selection
 * of similar jobs
 */
class SimulationSuiteReport(val responsesAll: ListBuffer[SimulationResponse], val config: EstimatorConfig, val simWeightsSolver: SimilarityWeightsSolver) extends Report {

  val minimalReport = true
  val failedResponses = responsesAll.filter(_.runtimeEstimate.equals(Double.NaN))
  val responses = responsesAll.filter(!_.runtimeEstimate.equals(Double.NaN)).sortBy(_.job.timeStart).reverse.sortBy(_.job.parameters).sortBy(_.job.input.name).sortBy(_.job.name)

  def createReport(): String = {
    templatePreCode()
    createHeading()
    createTable()
    templateAfterCode()
    return c.toString
  }
  def createHeading(): Unit = {
    c.append("<h1>Simulation Suite</h1>")
  }
  def createTable(): Unit = {
    val stat = responses.filter(r => !r.deviationPercentage.equals(java.lang.Double.NaN) && r.deviationPercentage != -1)

    if (!minimalReport) {
      c.append("<table style='font-size: 11px;'>")
      c.append(s"<tr>")
      c.append(s"<td>File</td>")
      c.append(s"<td>Experiment</td>")
      c.append(s"<td>Job</td>")
      c.append(s"<td>DatasetT</td>")
      c.append(s"<td>i</td>")
      c.append(s"<td>Actual</td>")
      c.append(s"<td>Estimate</td>")
      c.append(s"<td>Rel error</td>")
      c.append(s"<td>|Rel error|</td>")
      c.append(s"<td>Abs error</td>")
      c.append(s"<td>|Abs error|</td>")
      c.append(s"<td># jobs</td>")
      c.append(s"<td># jobs used</td>")
      c.append(s"<td>Lowered T</td>")
      c.append(s"<td>Runtime</td>")
      c.append(s"</tr>")
      responses.foreach(r => {
        if (r.failed) {
          c.append(s"<tr>")
          c.append(s"<td><a href='/file?file=${r.reportFileName}'>" + r.reportFileName.replace("simulationReport-", "") + "</a> &nbsp;</td>")
          c.append(s"<td><i>failed</i></td>")
          c.append(s"</tr>")
        } else {
          c.append(s"<tr>")
          c.append(s"<td><a href='/file?file=${r.reportFileName}'>${r.reportFileName}</a> &nbsp;</td>")
          c.append(s"<td>${r.job.expName} &nbsp;</td>")
          c.append(s"<td>${r.job.name} &nbsp;</td>")
          c.append(s"<td>${r.job.input.name} &nbsp;</td>")
          c.append(s"<td>${r.iteration} &nbsp;</td>")
          c.append(s"<td>" + "%.2f".format(r.runtimeActual) + " &nbsp;</td>")
          c.append(s"<td>" + "%.2f".format(r.runtimeEstimate) + " &nbsp;</td>")
          c.append(s"<td>" + "%.1f".format(r.deviationPercentage * 100) + "% &nbsp;</td>")
          c.append(s"<td>" + "%.1f".format(Math.abs(r.deviationPercentage * 100)) + "% &nbsp;</td>")
          c.append(s"<td>" + "%.2f".format(r.runtimeEstimate - r.runtimeActual) + " &nbsp;</td>")
          c.append(s"<td>" + "%.2f".format(Math.abs(r.runtimeEstimate - r.runtimeActual)) + " &nbsp;</td>")
          c.append(s"<td>${r.jobCount} &nbsp;</td>")
          c.append(s"<td>${r.jobCountSimilar} &nbsp;</td>")
          c.append(s"<td>${r.loweredThresholds} &nbsp;</td>")
          c.append(s"<td>" + "%.6f".format(r.simulationTime.toDouble / 1000 / 1000 / 1000) + " &nbsp;</td>")
          c.append(s"</tr>")
        }
      })
      c.append("</table>")
      c.append(s"<h3>Failed: ${failedResponses.length}</h3>")
      failedResponses.foreach(j => c.append(s"<br><a href='/file?file=${j.reportFileName}'>${j.job.id}</a> (" + j.job.name + ")"))
    }
    val useRelativeError = (r: SimulationResponse) => r.deviationPercentage * 100
    val useAbsoluteError = (r: SimulationResponse) => r.runtimeEstimate - r.runtimeActual
    val useRelativeErrorAbs = (r: SimulationResponse) => Math.abs(r.deviationPercentage * 100)
    val useAbsoluteErrorAbs = (r: SimulationResponse) => Math.abs(r.runtimeEstimate - r.runtimeActual)
    val useActual = (r: SimulationResponse) => r.runtimeActual
    val useEstimate = (r: SimulationResponse) => r.runtimeEstimate

    c.append("<h3>Statistics Rel Error [Abs]</h3>")
    deviationStats(c, stat, useRelativeErrorAbs, saveSummary=true)

    c.append("<h3>Statistics Abs Error [Abs]</h3>")
    deviationStats(c, stat, useAbsoluteErrorAbs)

    c.append("<h3>Statistics Actual</h3>")
    deviationStats(c, stat, useActual, scaleoutAnalysis = true)

    c.append("<h3>Statistics Estimate</h3>")
    deviationStats(c, stat, useEstimate, scaleoutAnalysis = true)

    c.append("<h3>Statistics Rel Error [+-]</h3>")
    deviationStats(c, stat, useRelativeError)

    c.append("<h3>Statistics Abs Error [+-]</h3>")
    deviationStats(c, stat, useAbsoluteError)

    addConfig()

  }

  private def deviationStats(c: StringBuilder, stat: ListBuffer[SimulationResponse], f: SimulationResponse => Double, scaleoutAnalysis: Boolean = false, saveSummary : Boolean = false) = {
    c.append("<table>")
    c.append(s"<tr>")
    c.append(s"<td>Dataset</td>")
    c.append(s"<td>Algo</td>")
    c.append(s"<td>n</td>")
    c.append(s"<td>mean</td>")
    c.append(s"<td>sd</td>")
    c.append(s"<td>confidence</td>")
    c.append(s"<td>confidence niveau</td>")
    c.append(s"<td>variance</td>")
    c.append(s"<td>range</td>")
    c.append(s"</tr>")
    val algoGroups = stat.groupBy(_.job.name)
    val values = stat.map(f(_))
    addStat(c, values, "all", "all", saveSummary)
    algoGroups.foreach(t => {
      val values = t._2.map(f(_))
      val algo = t._1
      addStat(c, values, algo, "all", saveSummary)
    })
    algoGroups.foreach(t => {
      val inputGroups = t._2.groupBy(_.job.input.name)
      val algo = t._1
      inputGroups.foreach(n => {
        val dataset = fixDataset(n._1)
        val values = n._2.map(f(_))
        addStat(c, values, algo, dataset, saveSummary)

      })
      //      if (scaleoutAnalysis) {
      val scaleOutGroups = t._2.map(r => {
        val s = r.job.iterations(0).resourceConfig.numberOfNodes
        val scaleout: Integer = if (s == 39) 40
        else if (s == 35) 36
        else if (s == 23) 24
        else s
        (scaleout, r)
      }).groupBy(_._1)
      scaleOutGroups.toList.sortBy(_._1).foreach(m => {
        val t = m._2.map(_._2)
        val values = t.map(f(_))
        val scaleout = m._1
        addStat(c, values, algo, "ScaleOut " + scaleout, false)
      })
      //      }
    })

    if (scaleoutAnalysis) {
      algoGroups.foreach(t => {
        val inputGroups = t._2.groupBy(_.job.input.name)
        val algo = t._1
        inputGroups.foreach(n => {
          val dataset = fixDataset(n._1)
          val scaleOutGroups = n._2.map(r => {
            val s = r.job.iterations(0).resourceConfig.numberOfNodes
            val scaleout: Integer = if (s == 39) 40
            else if (s == 35) 36
            else if (s == 23) 24
            else s
            (scaleout, r)
          }).groupBy(_._1)
          scaleOutGroups.toList.sortBy(_._1).foreach(m => {
            val t = m._2.map(_._2)
            val values = t.map(f(_))
            val scaleout = m._1
            addStat(c, values, algo, dataset + " [" + scaleout + "]", saveSummary)
          })
        })

      })

    }

    c.append("</table>")
  }

  private def fixDataset(dataset: String): String = {
    if (dataset.equals("gen-50-500000000")) {
      "50-500M"
    } else if (dataset.equals("gen-50-750000000")) {
      "50-750M"
    } else if (dataset.equals("gen-50-100M")) {
      "50-100M"
    } else if (dataset.equals("gen-25-500000000")) {
      "25-500M"
    } else if (dataset.equals("gen-25-250000000")) {
      "25-250M"
    } else {
      dataset
    }
  }

  val statSummary = ListBuffer[(String, Double)]()
  private def addStat(c: StringBuilder, values: ListBuffer[Double], algo1: String, dataset: String, saveSummary : Boolean = false) = {

    val algo = if (algo1.equals("GraphXPageRank")) {
      "PR"
    } else if (algo1.equals("GraphXConnectedComponents")) {
      "CC"
    } else if (algo1.equals("LinearRegression")) {
      "SGD"
    } else {
      algo1
    }
    val stats = new SummaryStatistics
    values.foreach(stats.addValue(_))
    c.append(s"<tr>")

    val ci = calcMeanCI(stats, 0.95)
    val mean = stats.getMean
    val lower = (mean - ci)
    val upper = (mean + ci)
    
    if(saveSummary) {
      if(dataset.equals("all")
        ||  (algo.equals("PR") && !dataset.startsWith("ScaleOut"))
      ) {
        val datasetC = if(dataset.equals("all")) "" else "-" + dataset
        val key = algo + datasetC
        statSummary += ((key, mean))
        
      }
    }

    c.append(s"<td>$algo</td>")
    c.append(s"<td>$dataset</td>")
    c.append(s"<td>${values.length}</td>")
    c.append(s"<td>" + "%.3f".format(mean) + "</td>")
    c.append(s"<td>" + "%.3f".format(stats.getStandardDeviation) + "</td>")
    c.append(s"<td>" + "%.3f".format(lower) + "-" + "%.3f".format(upper) + "</td>")
    c.append(s"<td>" + "%.3f".format(ci) + "</td>")
    c.append(s"<td>" + "%.3f".format(stats.getVariance) + "</td>")
    c.append(s"<td>" + "%.3f".format(stats.getMin) + "-" + "%.3f".format(stats.getMax) + "</td>")
    c.append(s"</tr>")
  }
  private def calcMeanCI(stats: SummaryStatistics, level: Double): Double = {
    try {
      val tDist = new TDistribution(stats.getN - 1)
      val critVal = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2)
      critVal * stats.getStandardDeviation() / Math.sqrt(stats.getN())
    } catch {
      case e: MathIllegalArgumentException => return Double.NaN
    }

  }
  private def addConfig() = {
    c.append("<h3>Config</h3>")
    for (v <- config.getAllValues()) {
      c.append(s"<div><span style='display: inline-block; width: 450px;'>${v._1}:</span> ${v._2}</div>")
    }
  }

  private def addDeviationAvg(deviationAvg: HashMap[String, (Double, Int)], r: SimulationResponse) = {
    addDeviationAvgKey("all", deviationAvg, r)
    addDeviationAvgKey("job-" + r.job.name, deviationAvg, r)
    addDeviationAvgKey("job-" + r.job.name + "-" + r.job.input.name, deviationAvg, r)

  }
  private def addDeviationAvgKey(key: String, deviationAvg: HashMap[String, (Double, Int)], r: SimulationResponse) = {
    if (r.deviationPercentage != Double.NaN) {
      var t = if (deviationAvg.contains(key)) deviationAvg.get(key).get else (.0, 0)
      deviationAvg.put(key, (t._1 + Math.abs(r.deviationPercentage), t._2 + 1))
    }
  }
}