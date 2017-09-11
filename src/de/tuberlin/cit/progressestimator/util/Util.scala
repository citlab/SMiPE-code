package de.tuberlin.cit.progressestimator.util

object Util {
  /**
   * create a md5 hash string of the give string.
   */
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  
  /**
   * return the sample iterations used for the simulation suites
   */
  def sampleIterations(i : Integer) : Array[Integer] = {
    
    val lowerBound = math.ceil(i * .15)
    val upperBound = math.floor(i * .85)
    val band = upperBound - lowerBound
    
    if(i < 20) {
      Array(i / 2)
    } else if(i < 40) {
      Array(math.ceil(lowerBound + band / 3).toInt, math.ceil(lowerBound + band * 2 / 3).toInt )
    } else {
      Array(math.ceil(lowerBound + band / 4).toInt, math.ceil(lowerBound + band * 2 / 4).toInt , math.ceil(lowerBound + band * 3 / 4).toInt )
    }
  }

}