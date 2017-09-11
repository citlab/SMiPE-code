package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import java.io.File
import java.io.PrintWriter
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import com.google.inject.Inject

class ReportsImpl @Inject() (  val config: EstimatorConfig) extends Reports {
  val REPORT_PATH = config.directoryReports
  
  def createJobReport(job: JobExecution): String = {

    val fileName = "job_" + job.id + "_" + job.name + ".html"
    val code = (new JobReport()).createReport(job)
    writeFile(code, fileName)
    return fileName
  }

  def writeFile(content: String, fileName: String): Unit = {

    val pw = new PrintWriter(new File(REPORT_PATH + "/" + fileName))
    try {
      pw.write(content)
    } finally {
      pw.close()
    }
  }
}