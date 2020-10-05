from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def get_empty_dataframe(spark):
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))

class SparkSingleton:
   """A singleton class which returns one Spark instance"""
   __instance = None
   @classmethod
   def get_instance(cls):
       """Create a Spark instance.

       :return: A Spark instance
       """
       if cls.__instance is None:
           cls.__instance = (SparkSession
                             .builder
                             .appName("code_challenge")
                             .getOrCreate())

       return cls.__instance
