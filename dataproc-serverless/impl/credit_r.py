from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def execute(spark):
    df_cust = spark.read.option('header', 'true').csv('gs://loan-details/loan-data/input/')
    df1 = df_cust.withColumn('name',f.concat(f.col('First Name'),f.lit(' '),f.col('Last Name')))
    df_final=df1.groupby('City').agg(f.count('name').alias('cust_count')).withColumn('load_date',f.current_date())
    df_final.write.mode('overwrite').partitionBy('load_date').parquet('gs://loan-details/loan-data//output')


def main():
    spark = SparkSession.builder.appName('test').getOrCreate()
    execute(spark)


if __name__ == '__main__':
    main()