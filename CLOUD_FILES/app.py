from neo4j import GraphDatabase
from fastapi import FastAPI
from typing_extensions import LiteralString, cast, List
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import json

load_dotenv()
neo4j_password: str | None = os.getenv("NEO4J_PASSWORD")

URI = "bolt://localhost:7687"
AUTH = ("neo4j", neo4j_password)

driver = GraphDatabase.driver(URI, auth=AUTH)
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("dataset/taxi_trips_clean.csv", header=True, inferSchema=True)
df2 = df.withColumn("fare_per_minute", df["fare"] / (df["trip_seconds"] / 60))
df2.createOrReplaceTempView("taxi_trips_clean")
app = FastAPI()

@app.get("/graph-summary")
def get_graph_sum():
    records, _, _ = driver.execute_query(
        """
        CALL {
            MATCH (d:Driver)
            RETURN count(d) AS driver_count
        }
        CALL {
            MATCH (c:Company)
            RETURN count(c) AS company_count
        }
        CALL {
            MATCH (a:Area)
            RETURN count(a) AS area_count
        }
        CALL {
            MATCH ()-[t:TRIP]->()
            RETURN count(t) AS trip_count
        }
        RETURN driver_count, company_count, area_count, trip_count
        """
    )

    counts = records[0]
    return {
        "driver_count": counts["driver_count"],
        "company_count": counts["company_count"],
        "area_count": counts["area_count"],
        "trip_count": counts["trip_count"]
    }

@app.get("/top-companies")
def get_top_companies(n: int = 1):
    n_str: str = str(n)
    value: LiteralString = cast(LiteralString, n_str)
    records, _, _ = driver.execute_query(
        f"""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->(a:Area)
        RETURN c.name AS name, count(*) AS trip_count
        ORDER BY trip_count
        DESC
        LIMIT {value}
        """
    )

    top_companies: List = [record.data() for record in records]

    return {
        "companies": top_companies
    }

@app.get("/high-fare-trips")
def get_high_fare_trips(area_id: int, min_fare: float):
    area_id_str: str = str(area_id)
    area_val: LiteralString = cast(LiteralString, area_id_str)
    min_fare_str: str = str(min_fare)
    min_fare_val: LiteralString = cast(LiteralString, min_fare_str)
    
    query: LiteralString = f"""
    MATCH (d:Driver)-[t:TRIP]->(:Area {{area_id: {area_val}}})
    WHERE t.fare > {min_fare_val}
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY fare DESC
    """
    records, _, _ = driver.execute_query(query)

    high_fares = [record.data() for record in records]
    return {
        "trips": high_fares
    }

@app.get("/co-area-drivers")
def get_co_drivers(driver_id:str):
    driver_id_lit: LiteralString = cast(LiteralString, driver_id)

    query: LiteralString = f"""
    MATCH (d1:Driver {{driver_id: '{driver_id_lit}'}})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC
    """

    results, _, _ = driver.execute_query(query)

    co_drivers = [record.data() for record in results]

    return {
        "co_area_drivers": co_drivers
    }

@app.get("/avg-fare-by-company")
def get_avg_fares():
    query: LiteralString = """
    MATCH (c:Company)<-[:WORKS_FOR]-(:Driver)-[t:TRIP]->(:Area)
    WITH c.name as name, avg(t.fare) as value
    RETURN name, ROUND(100 * value) / 100 AS avg_fare
    ORDER BY avg_fare DESC
    """

    results, _, _ = driver.execute_query(query)

    avg_fares = [record.data() for record in results]

    return {
        "companies": avg_fares
    }

@app.get("/area-stats")
def get_area_stats(area_id: int):
    area_id_str: str = str(area_id)
    area_id_lit: LiteralString = cast(LiteralString, area_id_str)
    query: LiteralString = f"""
    SELECT dropoff_area AS area_id, COUNT(*) AS trip_count, CAST(ROUND(AVG(fare), 2) AS INT) AS avg_fare, ROUND((AVG(trip_seconds))) AS avg_trip_seconds
    FROM taxi_trips_clean
    WHERE dropoff_area = {area_id_lit}
    GROUP BY dropoff_area 
    """
    result = spark.sql(query)
    json_res = result.toJSON().first()
    return json.loads(json_res) if json_res else None
    
@app.get("/top-pickup-areas")
def top_areas(n: int):
    n_str: str = str(n)
    n_lit: LiteralString = cast(LiteralString, n_str)
    query: LiteralString = f"""
    SELECT pickup_area, count(*) AS trip_count
    FROM taxi_trips_clean
    GROUP BY pickup_area
    ORDER BY trip_count DESC
    LIMIT {n_lit}
    """
    result = spark.sql(query)
    return {
        "areas": [row.asDict() for row in result.collect()]
    }

@app.get("/company-compare")
def compare_companies(company1: str, company2: str):
    query1: LiteralString = """
    SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare),2) AS avg_fare, ROUND(AVG(fare_per_minute),2) AS avg_fare_per_minute, CAST(ROUND(AVG(trip_seconds)) AS INT) AS avg_trip_seconds
    FROM taxi_trips_clean
    WHERE company = :company_name
    GROUP BY company
    LIMIT 1
    """
    query2: LiteralString = """
    SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare),2) AS avg_fare, ROUND(AVG(fare_per_minute),2) AS avg_fare_per_minute, CAST(ROUND(AVG(trip_seconds)) AS INT) AS avg_trip_seconds
    FROM taxi_trips_clean
    WHERE company = :company_name
    GROUP BY company
    LIMIT 1
    """

    result1 = spark.sql(query1, args={"company_name": company1})
    result2 = spark.sql(query2, args={"company_name": company2})

    if result1.isEmpty() or result2.isEmpty():
        return {
            "error": "one or more companies not found"
        }

    result1 = result1.first().asDict()
    result2 = result2.first().asDict()
    return {
        "comparison": [
            result1,
            result2
        ]
    }