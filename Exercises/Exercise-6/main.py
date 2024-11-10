from pyspark.sql import SparkSession, DataFrame

data = [[trip_id,start_time,end_time,bikeid,tripduration,from_station_id,from_station_name,to_station_id,to_station_name,usertype,gender,birthyear], [25223640,2019-10-01 00:01:39,2019-10-01 00:17:20,2215,940.0,20,Sheffield Ave & Kingsbury St,309,Leavitt St & Armitage Ave,Subscriber,Male,1987], [25223641,2019-10-01 00:02:16,2019-10-01 00:06:34,6328,258.0,19,Throop (Loomis) St & Taylor St,241,Morgan St & Polk St,Subscriber,Male,1998]]


def get_schema(spark: SparkSession):
    return spark.read.option("header", True).option("inferSchema", True).csv(data).schema

def get_dataframe(spark: SparkSession):
    return spark.read.csv("Exercises/Exercise-6/data/")


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    df = get_dataframe(spark)
    df.head()


    spark.stop()
    # your code here


if __name__ == "__main__":
    main()
