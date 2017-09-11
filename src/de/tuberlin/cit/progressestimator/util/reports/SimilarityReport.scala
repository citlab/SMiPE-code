package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * Create an html report with information on the selection
 * of similar jobs
 */
class SimilarityReport(currentJob: JobExecution, allJobs: ListBuffer[JobExecution], simResponse: SimulationResponse) extends Report {
  val useCharts = false
  createStats()
  def createReport(): String = {
    templatePreCode()
    createHeading()
    createLogs()
    createEventList()
    createAccordion()
    templateAfterCode()
    return c.toString()
  }
  def createHeading(): Unit = {
    c.append("<h1>Similarity Report</h1>")
    c.append(s"<div>Iterations: ${currentJob.iterations.size} / Total time: ${currentJob.runtime}</div>")
  }
  def createStats(): Unit = {
    simResponse.runtimeActual = currentJob.iterations.drop(simResponse.iteration).map(_.runtime).sum
    simResponse.addLog("Current Job", "iteration " + simResponse.iteration + ", data set " + currentJob.input.name + "</div>")
    simResponse.addLog("Other jobs ", "" + allJobs.size)
    simResponse.addLog("Actual Remaining Time", "%.3f".format(simResponse.runtimeActual))
  }

  def createLogs(): Unit = {

    c.append(s"""<div style="display: table;">""")
    simResponse.reportLogs.foreach(log =>
      c.append(s"""
            <div style="display: table-row;">
              <span style="display: table-cell; padding-right: 15px;">${log._1}</span>
              <span style="display: table-cell;">${log._2}</span>
            </div>
            """))
    c.append(s"</div>")

  }
  def createAccordion(): Unit = {
    c.append("""<script>
  $( function() {
    $( "#jobAccordion" ).accordion({
      animate: false,
      collapsible: true,
    });
  } );
  </script>""")
  }
  private def createJobTable(otherJob: JobExecution) = {
    c.append("<table>")
    c.append("<tr>")
    c.append(s"<td>i</td>")
    c.append(s"<td>current</td>")
    c.append(s"<td></td>")
    c.append(s"<td>other</td>")
    c.append(s"<td></td>")
    c.append(s"<td>adjusted</td>")
    c.append(s"<td>CF</td>")
    c.append(s"<td>CFweight</td>")
    c.append(s"<td></td>")
    c.append(s"<td></td>")
    c.append("</tr>")
    otherJob.iterations.foreach(o => {
      var currentRuntime = "-"
      var currentNodes = "-"
      var costFactor: Double = 0
      var iterationEstimate = ""
      var costFactorWeight = ""
      val jobEstimationDetails = simResponse.jobEstimationDetails.get(otherJob.id).get
      val iterationDetails = jobEstimationDetails.iterationDetails.apply(o.nr)
      if (o.nr < simResponse.iteration) {
        val c = currentJob.iterations.apply(o.nr)
        currentRuntime = c.runtime.toString
        currentNodes = c.resourceConfig.numberOfNodes.toString
        costFactorWeight = "%.3f".format(iterationDetails.costFactorWeight)
        costFactor = iterationDetails.costFactor
      } else {
        costFactor = jobEstimationDetails.costFactor
        iterationEstimate = "%.3f".format(costFactor * iterationDetails.adjustedRuntime)
        if (currentJob.iterations.length > o.nr) {
          val c = currentJob.iterations.apply(o.nr)
          currentRuntime = s"<span style='color: #c0c0c0'>${c.runtime}</span>"
          currentNodes = s"<span style='color: #c0c0c0'>${c.resourceConfig.numberOfNodes}</span>"
        }
      }
      c.append("<tr>")
      c.append(s"<td>${o.nr}</td>")
      c.append(s"<td>$currentRuntime</td>")
      c.append(s"<td>$currentNodes</td>")
      c.append(s"<td>${o.runtime}</td>")
      c.append(s"<td>${o.resourceConfig.numberOfNodes}</td>")
      c.append(s"<td>" + "%.3f".format(iterationDetails.adjustedRuntime) + "</td>")
      c.append(s"<td>" + "%.3f".format(costFactor) + "</td>")
      c.append(s"<td>$costFactorWeight</td>")
      c.append(s"<td>" + (if(iterationDetails.outlier) "[outlier]" else "") + "</td>")
      c.append(s"<td>${iterationEstimate}</td>")
      c.append("</tr>")
    })
    c.append("</table>")
  }
  def createEventList(): Unit = {
    c.append(s"<div id='jobAccordion'>")
    val logs = simResponse.reportJobLogs.toArray
    val orderedLogs = (logs.filter(t => !simResponse.jobEstimationDetails.get(t._1).get.discarded)
        ++ logs.filter(t => simResponse.jobEstimationDetails.get(t._1).get.discarded).toList)
    orderedLogs.foreach(
      m => {
        val jobId = m._1
        val logs = m._2
        val executionDetails = simResponse.jobEstimationDetails.get(jobId).get
        val jobExecution = executionDetails.job
        val discarded = executionDetails.discarded
        val discardedLabel = if (discarded) " [discarded]" else " [OK] - Sim " + "%.3f".format(executionDetails.similarity) + " - " + "Estimation " + 
           "%.3f".format(executionDetails.prediction)
        c.append(s"<h3 class='section'> Similarity with ${jobExecution.name} - ${jobId} (${jobExecution.input.name}$discardedLabel) " 
            + "</h3>")
        c.append(s"<div>")
        createJobTable(jobExecution)
        c.append(s"<div>Iterations: ${jobExecution.iterations.size} / Total time: ${jobExecution.runtime}</div>")
        c.append(s"""<div style="display: table;">""")
        logs.foreach(t => {
          c.append(s"""
            <div style="display: table-row;">
              <span style="display: table-cell; padding-right: 15px;">${t._1}</span>
              <span style="display: table-cell;">${t._2}</span>
            </div>
            """)
        })
        c.append(s"</div>")
        if (useCharts && !discarded) {
          c.append("<h4> Diagrams</h4>")
          simResponse.diagrams.get(jobId).get.foreach(
            t => {
                if (!t._3.isEmpty) {
                  c.append(s"<div style='font-size:12px;'>${t._3}</div>")
                }
                createGraphSameAxis(Seq(("Current Job", t._1), ("Job " + jobId, t._2)), float = true)
            })
        }
        c.append(s"</div>")
      })
    c.append(s"</div>")
  }
 

}