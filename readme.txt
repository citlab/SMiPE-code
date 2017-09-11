* This directory contains the implementation of the progress estimation system
  developed in the Master Thesis

  "Progress Estimation For Recurring Iterative Distributed Dataflows"

  by Jannis Koch, submitted 06/09/2017 at Technische Universität Berlin.

* Compile the project using Maven (mvn compile)

* Adjust the following parameters in the estimator.conf file:
  # estimator.directoryReports
  # estimator.directoryCaches
    These should point to directories for the reports and the caches, respectively.
  # db.union.urls
  	This list should point to database files OR directories with database files.
  	If a given database file does not exist, it is automatically created.

* To start the system, run the Scala main class
  de.tuberlin.cit.progressestimator.estimator.EstimatorMain

  and pass the location of the configuration file:
  -Dconfig.file=/path/to/estimator.conf
  An example estimator.conf is contained in this directory.

* The EstimatorMain class starts up a Web Server which provides a RESTful interface
  for the Estimator's functionality. Got to http://localhost:3000 for a Util interface
  which allows to access the functions via a browser.

* Consult the documentation provided via comments in the code or the Thesis for 
  more detailed information.
  
* The classes in the package de.tuberlin.cit.allocationassistant contain
  the implementation of the BELL system by Thamsen et al.:
  
  Thamsen, L., Verbitskiy, I., Schmidt, F., Renner, T., & Kao, O. (2016, December). 
  Selecting resources for distributed dataflow systems according to runtime targets. 
  In Performance Computing and Communications Conference (IPCCC), 
  2016 IEEE 35th International (pp. 1-8). IEEE.
  
  They contain minor changes to work with the progress estimation system.