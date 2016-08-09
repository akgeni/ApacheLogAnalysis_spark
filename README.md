# ApacheLogAnalysis_spark

Log analysis can open door all sort of business intelligence. like 

###Price Optimzation -

###Recommendation - 

Log Analysis can yield some insight like, what all resources a user is accessing or have accessed.
Based on these results we can recommend related resources.


####TODO
(1). Find endpoints that are being accessed most.

(2). Find enpoints that are being accessed most per user.

(3). Use IP address to trace user locations, so that some location-based decision could be taken.

(4). Add plots(matplotlib) to details the result


###How to run
./spark-submit --py-files apache_access_log.py spark_log_analyzer.py access_log

