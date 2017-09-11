package de.tuberlin.cit.progressestimator.rest

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer
import scala.language.existentials



import com.google.inject.Inject
/**
 * Very simple web server which takes HTTP requests for the REST API.
 *
 */
import com.google.inject.Singleton

import de.tuberlin.cit.progressestimator.estimator.DynamicSimilarityThresholdExtractor
import de.tuberlin.cit.progressestimator.estimator.Estimator
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import de.tuberlin.cit.progressestimator.estimator.SimilarityWeightsSolver
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.similarity.DstatCache
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.simulator.Simulator
import de.tuberlin.cit.progressestimator.util.reports.Reports
import de.tuberlin.cit.progressestimator.util.reports.SimulationSuiteReport
import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD._
import de.tuberlin.cit.progressestimator.util.Util
import de.tuberlin.cit.progressestimator.simulator.SimulationRequest


@Singleton
class SimulationService @Inject() (val reports: Reports, val jobHistory: JobHistory, val simulator: Simulator,
                                   val estimator: Estimator, val simThresholdExtractor: DynamicSimilarityThresholdExtractor,
                                   val simQualService: SimilarityQualityService,
                                   val config: EstimatorConfig, val dstatCache: DstatCache, val weightsSolver: SimilarityWeightsSolver) {

  def serveSimulationSuite(session: IHTTPSession, trainWeights: Boolean = true, suiteStatistics: ListBuffer[(String, Double)] = null): String = {
    val simulationResponses = ListBuffer[SimulationResponse]()

    val parameters = session.getParms
    var saveTrainedWeights = false
    if (parameters.containsKey("dummy")) {
      println("Use dummy simulation suite")
      val allExec = jobHistory.getJobExecutions()
      val selected = allExec.take(25)

      simulationHelperSeq(simulationResponses, weightsSolver, selected.map(_.id.toInt).toSeq)

    } else {

      saveTrainedWeights = true
      val selected = jobHistory.getJobExecutions()

      simulationHelperSeq(simulationResponses, weightsSolver, selected.map(_.id.toInt).toSeq)
    }

    val suiteReport = new SimulationSuiteReport(simulationResponses, config, weightsSolver)
    val code = suiteReport.createReport()
    val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm").withZone(ZoneOffset.ofHours(2)).format(Instant.now())
    val fileName = "simulationSuite-" + timestamp + ".html"
    reports.writeFile(code, fileName)
    if (suiteStatistics != null) {
      suiteStatistics ++= suiteReport.statSummary
    }
    if (trainWeights) {
      weightsSolver.solve(saveTrainedWeights)
    }
    fileName
  }

  private def simulationHelperSeq(simulationResponses: ListBuffer[SimulationResponse], weightsSolver: SimilarityWeightsSolver, ids: Seq[Int]) = {
    ids.foreach(simulationHelper(simulationResponses, weightsSolver, _))

  }
  private def simulationHelper(simulationResponses: ListBuffer[SimulationResponse], weightsSolver: SimilarityWeightsSolver, id: Int) = {
    val job = jobHistory.getJobExecutionById(id)
    val createReport = config.simulationSuiteCreateIndividualReports
    if (job != null) {

      val iterations = Util.sampleIterations(job.iterations.length)
      for (i <- iterations) {
        simulationResponses += simulator.simulate(new SimulationRequest(i, id, weightsSolver), createReport)
      }
    }
  }
}