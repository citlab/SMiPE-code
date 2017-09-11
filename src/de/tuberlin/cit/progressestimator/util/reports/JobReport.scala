package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer

/**
 * Create an html report with information on a job execution
 */
class JobReport extends Report {
  def createReport(job: JobExecution): String = {
    templatePreCode()
    createHeading(job)
    createIterationGraph(job)
    createDstatGraph(job)
    createIterationTable(job)
    templateAfterCode()
    return c.toString;
  }
  def createHeading(job: JobExecution): Unit = {
    c.append("<h1>Job Report: " + job.name + "</h1>")
    c.append("<div>Source: " + job.source + "</div>")
    c.append("<div>Runtime: " + job.runtime + "</div>")
    c.append("<div>Parameters: " + job.parameters + "</div>")
    c.append("<div>Experiment Name: " + job.expName + "</div>")
    c.append("<div>Hash: " + job.hash + "</div>")
    c.append("<div>Input Name: " + job.input.name + "</div>")
    c.append("<div>Input File: " + job.input.file + "</div>")
    c.append("<div>Input Records: " + job.input.numberOfRecords + "</div>")
    c.append("<div>Dstat entries: " + job.dstatEntries.entries.length + "</div>")
    c.append("<div>Avg Idl: " + job.dstatEntries.getAvg("idl") + "</div>")
    c.append("<div>Avg Writ: " + (job.dstatEntries.getAvg("writ") / 1000).toInt + "</div>")
    c.append("<div>Avg Recv: " + (job.dstatEntries.getAvg("recv") / 1000).toInt + "</div>")
    c.append("<div>Avg Send: " + (job.dstatEntries.getAvg("send") / 1000).toInt + "</div>")
    c.append("<div>Iterations: " + job.iterations.length + "</div>")
    c.append("<div>Start: " + job.timeStart + "</div>")
    c.append("<div>Finish: " + job.timeFinish + "</div>")
    c.append("<div>Completed: " + job.finished + "</div>")
  }
  def createIterationTable(job: JobExecution): Unit = {
    c.append("""
      <h2>Iterations</h2>
        <table>
          <tr>
            <td width=40>#</td>
            <td width=100>Runtime</td>
            <td width=100># Active Records</td>
            <td width=100># Nodes</td>
          </tr>
        """ +
      job.iterations.map(i => """
            <tr>
              <td>""" + i.nr + """</td>
              <td>""" + "%.2f".format(i.runtime) + """</td>
              <td>""" + i.input.numberOfRecords + """</td>
              <td>""" + i.resourceConfig.numberOfNodes + """</td>
            </tr>
          """).mkString("")
      + """
        </table>
      """)
  }
  def createIterationGraph(job: JobExecution): Unit = {
    var dataRuntime = job.iterations.map(_.runtime)
    var dataRecords = job.iterations.map(_.input.numberOfRecords + .0)
    createGraph(Seq(("Runtime", dataRuntime), ("Active Records", dataRecords)))
  }
  def createDstatGraph(job: JobExecution): Unit = {
    c.append("""<h2>Dstat</h2>""")
    val dstatEntries = job.dstatEntries
    for (hostEntries <- dstatEntries.entries.groupBy(_.host)) {
      c.append(s"<h4>${hostEntries._1}</h4>")
      c.append(s"<div>Avg idl: " + dstatEntries.getAvgByHost("idl", hostEntries._1) +" </div>")
      c.append(s"<div>Avg writ: " + (dstatEntries.getAvgByHost("writ", hostEntries._1) / 1000).toInt +" </div>")
      c.append(s"<div>Avg recv: " + (dstatEntries.getAvgByHost("recv", hostEntries._1) / 1000).toInt +" </div>")
      c.append(s"<div>Avg send: " + (dstatEntries.getAvgByHost("send", hostEntries._1) / 1000).toInt +" </div>")
      val data = hostEntries._2.groupBy(_.property).slice(0,120).map(t => (t._1, t._2.map(_.value))).toList
      val ordered = orderDstatEntries(data)
      createGraph(ordered)
    }
  }
  private val order = Seq("idl", "recv", "send", "read", "write")
  private def orderDstatEntries(data: Seq[(String, Array[Double])]) : Seq[(String, ListBuffer[Double])] = {
      val keys = data.map(_._1)
      val orderExtended = order.union(keys.diff(order))
      val dataMap = data.map(w => w._1 -> w).toMap
      val ordered = orderExtended.collect(dataMap).map(t => (t._1, t._2.to[ListBuffer]))
      ordered
  }
}