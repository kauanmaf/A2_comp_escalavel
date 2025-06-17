from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def join(df1, df2, id):
    df_joined = df1.join(df2, on = id)
    return df_joined