from neo4j import GraphDatabase
from fastapi import FastAPI
from typing_extensions import LiteralString, cast, List
import os
from dotenv import load_dotenv

load_dotenv()
neo4j_password: str | None = os.getenv("NEO4J_PASSWORD")

URI = "bolt://localhost:7687"
AUTH = ("neo4j", neo4j_password)

driver = GraphDatabase.driver(URI, auth=AUTH)
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

@app.get("/node-count")
def get_count():
    records, _, _ = driver.execute_query(
        """
            MATCH (d)
            RETURN count(d) AS total_nodes
        """
    )
    counts = records[0]

    return {
        "total_nodes": counts['total_nodes']
    }

@app.get("/top-companies")
def get_top_companies(n: int = 1):
    n_str: str = str(n)
    value: LiteralString = cast(LiteralString, n_str)
    records, _, _ = driver.execute_query(
        f"""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->(a:Area)
        RETURN c AS company_name, count(*) AS trip_count
        ORDER BY trip_count
        DESC
        LIMIT {value}
        """
    )

    top_companies: List = [record.data() for record in records]

    return {
        "companies": top_companies
    }