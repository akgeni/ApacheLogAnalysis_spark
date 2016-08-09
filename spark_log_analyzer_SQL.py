
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import apache_access_log
import sys


conf = SparkConf().setAppName("Log Analyzer SQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

logFile = sys.argv[1]

# Caching RDD of log files, we need to perform many operations on it.
access_logs = (sc.textFile(logFile)
                 .map(apache_access_log.parse_apache_log_line)
                 .cache())


schema_access_logs = sqlContext.createDataFrame(access_logs)
schema_access_logs.createOrReplaceTempView("logs")

content_size_stats = (sqlContext
                       .sql("SELECT %s, %s, %s, %s FROM logs" % (
                            "SUM(content_size) as theSum",
                            "COUNT(*) as theCount",
                            "MIN(content_size) as theMin",
                            "MAX(content_size) as theMax"))
                       .first())

print("Content avg size %d, Min %d, Max %d" % 
      (content_size_stats[0] / content_size_stats[1],
      content_size_stats[2],
      content_size_stats[3]))

# Lets find out the IP address that have accessed the server more than 10 times.

ipAddresses = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100"))

print("All ipAddresses accessed more than 10 times %s " % ipAddresses.collect())

'''
Output - 

[Row(ip_address=u'cr020r01-3.sac.overture.com', total=44), Row(ip_address=u'216-160-111-121.tukw.qwest.net', total=12), Row(ip_address=u'ip68-228-43-49.tc.ph.cox.net', total=22), Row(ip_address=u'h24-71-236-129.ca.shawcable.net', total=36), Row(ip_address=u'pc3-registry-stockholm.telia.net', total=13), Row(ip_address=u'128.227.88.79', total=12), Row(ip_address=u'ogw.netinfo.bg', total=11), Row(ip_address=u'203.147.138.233', total=13), Row(ip_address=u'ts04-ip92.hevanet.com', total=28), Row(ip_address=u'p213.54.168.132.tisdip.tiscali.de', total=12), Row(ip_address=u'mail.geovariances.fr', total=14), Row(ip_address=u'ts05-ip44.hevanet.com', total=14), Row(ip_address=u'ns.wtbts.org', total=12), Row(ip_address=u'prxint-sxb3.e-i.net', total=14), Row(ip_address=u'10.0.0.153', total=188), Row(ip_address=u'200-55-104-193.dsl.prima.net.ar', total=13), Row(ip_address=u'195.246.13.119', total=12), Row(ip_address=u'market-mail.panduit.com', total=29), Row(ip_address=u'h24-70-69-74.ca.shawcable.net', total=32), Row(ip_address=u'lhr003a.dhl.com', total=13), Row(ip_address=u'', total=139), Row(ip_address=u'212.92.37.62', total=14), Row(ip_address=u'208-38-57-205.ip.cal.radiant.net', total=11), Row(ip_address=u'207.195.59.160', total=15), Row(ip_address=u'proxy0.haifa.ac.il', total=19), Row(ip_address=u'64.242.88.10', total=452)] 
'''
