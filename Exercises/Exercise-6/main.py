from enum import EnumType

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, StringType
import os, zipfile
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

schema_1 = StructType([
    StructField("start_station_id", IntegerType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("end_station_id", IntegerType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("ended_at", TimestampType(), True),
])
schema_2 = StructType([
    StructField("birthyear", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("tripduration", StringType(), True), # TODO: Requires conversion to DoubleType but some values are stringified
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("trip_id", IntegerType(), True),
])

def read_zipped_csvs_in_memory(spark: SparkSession, folder: str) -> list[DataFrame]:
    """
    Reads all .zip files from `folder`, finds any .csv files inside each .zip,
    and returns a unioned DataFrame of all CSVs.
    """
    dfs = []
    for file_name in os.listdir(folder):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(folder, file_name)
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Loop over each file inside the zip
                for zipped_file in zf.namelist():
                    if zipped_file.endswith(".csv") and not zipped_file.startswith("._") and "__MACOSX" not in zipped_file:
                        # Read the CSV lines (in memory)
                        with zf.open(zipped_file) as f:
                            # f.read() -> raw bytes
                            # decode -> string
                            # splitlines -> list of CSV lines
                            csv_data = f.read().decode("utf-8").splitlines()
                            # parallelize the lines
                            rdd = spark.sparkContext.parallelize(csv_data)
                            # read them as CSV
                            tmp_df = spark.read \
                                          .csv(rdd, header=True)
                            dfs.append(tmp_df)
    if len(dfs) == 0:
        return None
    return dfs


def standardize_columns(df_1, df_2, schema_1=schema_1, schema_2=schema_2):
    # TODO: Implement the function so that the columns of the two DataFrames are standardized and match the schemas
    #       provided in the arguments.
    #       The function should return the standardized DataFrames.

    # Insert Code Here

    return df_1, df_2


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    df_1,df_2 = read_zipped_csvs_in_memory(spark, "Exercises/Exercise-6/data")
    logger.info(print(sorted(df_1.columns))) # TODO: Remove
    logger.info(print(sorted(df_2.columns))) # TODO: Remove

    df_1,df_2 = standardize_columns(df_1,df_2)


    spark.stop()

if __name__ == "__main__":
    main()
