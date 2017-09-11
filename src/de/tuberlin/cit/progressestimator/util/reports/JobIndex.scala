package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import java.time.format.DateTimeFormatter
import java.time.ZoneId

/**
 * Create an html report with information on a job execution
 */
class JobIndex extends Report {
  def createReport(jobsWithFileName : Seq[(JobExecution, String)]) : String = {
    templatePreCode() 
    c.append(s"<h1>All Jobs (${jobsWithFileName.size})</h1>")
    c.append("""<table>
                <tr>
                  <td></td>
                  <td>ID</td>
                  <td>Date</td>
                  <td>Name</td>
                  <td>Experiment</td>
                  <td>Input</td>
                  <td>Runtime</td>
                  <td>Iterations</td>
                </tr>""")
    
    val formatter = DateTimeFormatter.ofPattern("dd.MM.yy HH:mm").withZone(ZoneId.systemDefault())
    var i = 0
    jobsWithFileName.foreach(t => {
      i+= 1
      val job = t._1
      val filename = t._2
      var date = "###"
      try {
        date = formatter.format(job.timeStart)
      } catch {
           case e: Exception => date= e.getMessage
      }
      var styleGrey = ""
      if(!job.finished) {
        styleGrey = """ style="color: grey; " """
      }
      if(job.outlier) {
        styleGrey = """ style="color: lightcoral; " """
      }
      c.append(s"""<tr${styleGrey}>
          <td>${i}</td>
          <td><a href='${filename}'>Job ${job.id}</a></td>
          <td>${date}</td>
          <td>${job.name}</td>
          <td>${job.expName}</td>
          <td>${job.input.name}</td>
          <td>${job.runtime}</td>
          <td>${job.iterations.size}</td>
          </tr>""")
    })
    c.append("</table>")
    
    templateAfterCode()
    return c.toString
  }
  
}