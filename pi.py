import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    spark.sparkContext.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext.hadoopConfiguration().set("spark.driver.extraClassPath", "/opt/spark/jars/*")
    spark.sparkContext.hadoopConfiguration().set("spark.executor.extraClassPath", "/opt/spark/jars/*")
    spark.sparkContext.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AKIAWR6Y3LBV4FF42GCK")
    spark.sparkContext.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "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")

    myfile = spark.sparkContext.textFile("s3a://pro-pair-serverless/staging/test/myfile.txt")
    myfile.saveAsTextFile("s3a://pro-pair-serverless/staging/test/test/myoutfile.txt")

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Jimmy was here !!!!")
    print("Pi is roughly %f" % (4.0 * count / n))
    
    spark.stop()
