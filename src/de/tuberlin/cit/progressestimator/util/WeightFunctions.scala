package de.tuberlin.cit.progressestimator.util

object WeightFunctions {
  
  var weightFunction1 = (lastIteration: Int, x: Int) => 1.0
  var weightFunction1OverX = (lastIteration: Int, x: Int) => weightFunction1OverRootX(1, lastIteration, x)
  var weightFunction1OverSquareRootX = (lastIteration: Int, x: Int) => weightFunction1OverRootX(2, lastIteration, x)
  
  var weightFunctionLinear = (lastIteration: Int, x: Int) => {
    
    val x1 = (x - lastIteration)
    
    1.0 - 1.0 * x1 / lastIteration
    
  }
  def weightFunction1OverRootX = (root : Double, lastIteration: Int, x: Int) => {
    val x1 = (x - lastIteration - 1) * -1
    if (x1 == 0) {
      0
    } else if(root > 1 && x1 < 0 ) {
      // negative root
      0
    } else {
      1.0 / Math.pow(x1, 1 / root)
    }
  }
  
  var weightFunctionGaussian = (lastIteration: Int, x: Int) => {
    val x1 = x - lastIteration + 1
    val base = .2
    val a = 1- base
    val b = .5
    val c = 7
    val gauss = base + a * Math.exp(-1 * Math.pow((x1 - b), 2) / (2 * Math.pow(c,2)))
    gauss
  }

}