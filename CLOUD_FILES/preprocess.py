from pyspark.sql import SparkSession
from typing_extensions import LiteralString

folder: str = "dataset"

def main():
    spark = SparkSession\
        .builder\
        .appName("CS498 HW4 Part 2")\
        .getOrCreate()
    
    # reading csv file
    df = spark.read.csv(f"{folder}/taxi_trips_clean.csv", header=True, inferSchema=True)
    print("read csv file!\n")

    # Adding column
    df = df.withColumn("fare_per_minute", df["fare"] / (df["trip_seconds"] / 60))

    # SQL View Registration
    df.createOrReplaceTempView("trips")

    # computing company-level summary
    query: LiteralString = """
    SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare), 2) AS avg_fare, ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
    FROM trips
    GROUP BY company
    ORDER BY trip_count DESC
    """
    results = spark.sql(query)

    result_json = results.toJSON()

    result_json.saveAsTextFile("processed_data")

if __name__ == "__main__":
    main()
