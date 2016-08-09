from pyspark import SparkContext, SparkConf
import os



import apache_access_log
import sys


conf = SparkConf().setAppName('Iscsi Log Analyzser')
sc = SparkContext(conf=conf)

inputLogFile = sys.argv[1]

# ask spark to cache access_logs so that, future actions will be faster.
access_logs = (sc.textFile(inputLogFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache()
              )

# Lets find all content size
content_sizes = access_logs.map(lambda logLine: logLine.content_size).cache()

print ("Content size avg %i, Min: %i, Max: %s" % (
       content_sizes.reduce(lambda a, b : a+b) / content_sizes.count(),
       content_sizes.min(),
       content_sizes.max()
       ))

# top ip adress
top_ips = (access_logs.map(lambda logLine: (logLine.ip_address, 1))
                      .reduceByKey(lambda a, b: a + b)
                      .cache())
print ("top 10 ips  ", top_ips.takeOrdered(10, lambda x: -x[1]))



# response code returned by server
response_codes = (access_logs.map(lambda logLine: (logLine.response_code, 1))
                            .reduceByKey(lambda x, y: x + y))

print ('status_codes ', response_codes.collect())


# top resources being accessed
top_end_points = (access_logs.map(lambda logLine: (logLine.endpoint, 1))
                               .reduceByKey(lambda x, y : x + y))
print ('top resources ', top_end_points.takeOrdered(10, lambda x: -x[1]))
                               

