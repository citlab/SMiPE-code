package de.tuberlin.cit.progressestimator.util.reports
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.entity.JobExecution
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.Locale
import scala.collection.mutable.AbstractBuffer

abstract class Report {

  val c = new StringBuilder
  def createGraphSameAxis(data: Seq[(String, ListBuffer[Double])], float: Boolean = false): Unit = {
    createGraphHelper(data, sameAxis = true, float = float)
  }
  def createGraph(data: Seq[(String, ListBuffer[Double])], float: Boolean = false, labels: ListBuffer[Double] = null): Unit = {
    createGraphHelper(data, float = float, labels=labels)
  }
  def createScatterGraph(data: Seq[(String, AbstractBuffer[(Double, Double)])], float: Boolean = false, add0011 :Boolean= false): Unit = {
    createScatterGraphHelper(data, float = float, add0011= add0011)
  }
  private def createGraphHelper(data: Seq[(String, ListBuffer[Double])], sameAxis: Boolean = false, float: Boolean = false, labels: ListBuffer[Double] = null): Unit = {
    val dataCount = data.map(_._2.size).reduce(_ max _)
    val id = scala.util.Random.nextInt(100000000)
    var wrapperStyle = ""
    if (float) {
      wrapperStyle = " float: left;width: 50%; min-width: 300px;"
    }
    var yLabels = ListBuffer.range(0, dataCount - 1).map(_.toDouble)
    if (labels != null) {
      yLabels = labels
    }
    c.append(s"""<!-- [CHARTBEGIN $id --><div data-uid='$id' class='chartWithScript'>
      <div style='$wrapperStyle'>
    		<div class='chart-wrapper' id='chart-wrapper-${id}'>
    			 <canvas id='chart-${id}' width="400" height="100"></canvas>
    		</div>
  		</div>
  		<script>
		""" + '$' + s"""('#chart-wrapper-${id}').mouseover(function() {
		var ctx = document.getElementById('chart-${id}');
		""" + '$' + s"""(this).unbind('mouseenter mouseleave mouseover');
		var myChart = new Chart(ctx, {
		    type: 'line', maintainAspectRatio: false,
		    data: {
		        labels: [ """ + yLabels.map("%.2f".format(_)).mkString(", ") + """],
		         datasets: [  
		          """)
    var i = 0
    for (dataSet <- data) {
      i += 1
      c.append(s"""{   """ + createGraphDataSetOptions(i) + """
		            label: '""" + dataSet._1 + """', 
		            data: [""" + dataSet._2.mkString(", ") + """],""")

      if (sameAxis) {
        c.append(s"yAxisID: 'y',")
      } else {
        c.append(s"yAxisID: 'y-$i',")
      }
      if (i > 3) {
        c.append("hidden: true, ")
      }
      c.append("},")
    }

    c.append("""]
		    },
		    options: {
             animation: {
      				duration: 0,
      				onComplete: onChartComplete
      			},
            showTooltips: false,
		        scales: {
		            yAxes: [""")
    if (sameAxis) {
      c.append("{id: 'y', ticks: { beginAtZero:true } }")
    } else {
      i = 0
      for (dataSet <- data) {
        i += 1
        c.append(s"{id: 'y-$i', ticks: { beginAtZero:true } ")
        if (i % 2 == 0) {
          c.append(", position: 'right'")
        }
        if (i > 2) {
          c.append(", display: false")

        }
        c.append("},")
      }
    }

    c.append("""],
		            xAxes: [{
		            }]
		        }
		    }
		});
		});
		</script>
		</div><!-- ]CHARTEND $id -->
  		""")
  }
  private def formatValue(v : Double) : String = {
    val otherSymbols :DecimalFormatSymbols = new DecimalFormatSymbols(Locale.US)
    otherSymbols.setDecimalSeparator('.')
    otherSymbols.setGroupingSeparator(',')
    try {
    (new DecimalFormat("#.####", otherSymbols)).format(v)
    } catch {
      case e : Exception => e.printStackTrace()
      "0"
    }
  }

   private def createScatterGraphHelper(data: Seq[(String, AbstractBuffer[(Double, Double)])], sameAxis: Boolean = false, float: Boolean = false, add0011 : Boolean = false): Unit = {
    val dataCount = data.map(_._2.size).reduce(_ max _)
    val id = scala.util.Random.nextInt(100000000)
    var wrapperStyle = ""
    if (float) {
      wrapperStyle = " float: left;width: 50%; min-width: 300px;"
    }
    c.append(s"""<!-- [CHARTBEGIN $id --><div data-uid='$id' class='chartWithScript'>
      <div style='$wrapperStyle'>
  		<div class='chart-wrapper' id='chart-wrapper-${id}'>
  			 <canvas id='chart-${id}' width="400" height="100">
  		</div>
  		</div>
  		<script>
		var ctx = document.getElementById('chart-${id}');
		var myChart = new Chart(ctx, {
		    type: 'bubble', maintainAspectRatio: false,
		    data: {
		        label: [ "Label"],
		         datasets: [  
		          """)
    var i = 0
    for (dataSet <- data) {
      i += 1
      c.append(s"""{   """ + createGraphDataSetOptions(i) + """
		            label: '""" + dataSet._1 + """',
		            data: [""" 
		            + (if(add0011) "{x: 0, y: 0}, {x: 1, y: 1}, " else "")
		            + dataSet._2.map(t => s"{x: " + formatValue(t._1) + s",y: " + formatValue(t._2) + s" }").mkString(", ") + """],""")

      if (sameAxis) {
        c.append(s"yAxisID: 'y',")
      } else {
        c.append(s"yAxisID: 'y-$i',")
      }
      if (i > 3) {
        c.append("hidden: true, ")
      }
      c.append("},")
    }

    c.append("""]
		    },
		    options: {
             animation: {
      				duration: 0,
      				onComplete: onChartComplete
      			},
            showTooltips: false,
		        scales: {
		            yAxes: [""")
    if (sameAxis) {
      c.append("{id: 'y', ticks: { beginAtZero:true } }")
    } else {
      i = 0
      for (dataSet <- data) {
        i += 1
        c.append(s"{id: 'y-$i', ticks: { beginAtZero:true } ")
        if (i % 2 == 0) {
          c.append(", position: 'right'")
        }
        if (i > 2) {
          c.append(", display: false")

        }
        c.append("},")
      }
    }

    c.append("""],
		            xAxes: [{
                type: 'linear',
                position: 'bottom'
		            }]
		        }
		    }
		});
		</script>
		</div><!-- ]CHARTEND $id -->
  		""")
  }

  def createGraphDataSetOptions(color: Int, transparentDots : Boolean = true): String = {
    var colorString = (color % 5) match {
      case 0 => "255,102,0"
      case 1 => "0,102,255"
      case 2 => "204, 0, 0"
      case 3 => "0, 204, 163"
      case 4 => "204, 204, 0"
      case _ => "75,192,192"
    }
    val opacity = if(transparentDots) {
      0.2
    } else {
      1
    }
    return s""" fill: false,
		            lineTension: 0.1,
		            backgroundColor: "rgba($colorString , .1)",
		            borderColor: "rgba($colorString , $opacity)",
		            borderCapStyle: 'butt',
		            borderDash: [],
		            borderDashOffset: 0.0,
		            borderJoinStyle: 'miter',
		            pointBorderColor: "rgba($colorString , $opacity)",
		            pointBackgroundColor: "#fff",
		            pointBorderWidth: 3,
		            pointHoverRadius: 5,
		            pointHoverBackgroundColor: "rgba($colorString ,1)",
		            pointHoverBorderColor: "rgba(220,220,220,1)",
		            pointHoverBorderWidth: 2,
		            pointRadius: 1,
		            pointHitRadius: 10,"""
  }
  def templatePreCode(): Unit = {
    
    c.append("<html><head>"
      + "<script src='https://code.jquery.com/jquery-3.2.0.min.js'"
      + " integrity='sha256-JAW99MJVpJBGcbzEuXk4Az05s/XyDdBomFqNlM3ic+I='"
      + " crossorigin='anonymous'></script>"
      + "<script src='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.5.0/Chart.bundle.min.js'"
      + " crossorigin='anonymous'></script>"
      + "<script src='https://cdnjs.cloudflare.com/ajax/libs/jquery.lazyload/1.9.1/jquery.lazyload.min.js'></script>"
      + "<script src='https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.1/jquery-ui.min.js'></script>"
      + "<link rel='stylesheet' href='https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.1/jquery-ui.min.css'>"
      + "<style>" + getCss() + "</style>"
      + "<script>" + getJs() + "</script>"
      + " </head>"
      + "<body>")
  }
  def templateAfterCode(): Unit = {
    c.append("</body></html>")
  }
  def getCss(): String = {
    return """
        td { padding-right: 12px; }
        h1,h2,h3,h4 {clear: left; }
      """
  }
  def getJs(): String = {
    return """
      standardOptions  = {
      };
      graphOptions = []
      $(function() {
  	    $(".chart-wrapper").lazyload({
  	        threshold : 1000,
  	        callback : function(element) {
  	        console.log(element.getAttr('id'));
  	            var ctx = element.find('canvas').get();
  	            var options = MergeRecursive(standardOptions, graphOptions[element.getAttr('id')]);
  	            setTimeout(function () {
  		        	  var myChart = new Chart(ctx, options);
  	                }, 1);
  	        }
  	    });
    	});
      
      var onChartComplete = function (animation) { 
      					var base = this.toBase64Image();
      					var imgSrc = "<img width='100%' src='" + base + "' />";
      					var w = $(ctx).closest('.chartWithScript');
      					var uid = w.attr('data-uid');
      					w.after(imgSrc);
      					w.remove();
      					
      	};  
/*
* Recursively merge properties of two objects 
http://stackoverflow.com/questions/171251/how-can-i-merge-properties-of-two-javascript-objects-dynamically
*/
function MergeRecursive(obj1, obj2) {

  for (var p in obj2) {
    try {
      // Property in destination object set; update its value.
      if ( obj2[p].constructor==Object ) {
        obj1[p] = MergeRecursive(obj1[p], obj2[p]);

      } else {
        obj1[p] = obj2[p];

      }

    } catch(e) {
      // Property in destination object not set; create it and set its value.
      obj1[p] = obj2[p];

    }
  }

  return obj1;
}

      """
  }
}