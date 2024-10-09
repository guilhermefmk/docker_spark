# CLUSTER SPARK ON DOCKER

https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b

To acess the SparkSession of cluster you need to use the ip of docker0 interface

> ifconfig docker0

inet 172.17.0.1

spark = SparkSession.builder \
    .appName("Jupyter Spark Example") \
    .master("spark://172.17.0.1:7077").getOrCreate()