import sys
from typing import LiteralString

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

def import_csv(tx):
    import_query: LiteralString = """
        LOAD CSV WITH HEADERS FROM 'file:///taxi_trips_clean.csv' AS row
        MERGE (d:Driver {driver_id: row.driver_id})
        MERGE (c:Company {name: row.company})
        MERGE (a:Area {area_id: toInteger(row.dropoff_area)})
        MERGE (d)-[:WORKS_FOR]->(c)
        MERGE (d)-[:TRIP {trip_id: row.trip_id, fare: toFloat(row.fare), trip_seconds: toInteger(row.trip_seconds)}]->(a)
        """
    tx.run(import_query)

def main() -> None:
    load_dotenv()
    with open("HW4.txt", "r") as ip_file:
        external_ip: str = ip_file.readline()
    neo4j_pw: str | None = os.getenv("NEO4J_PW")
    if neo4j_pw is None:
        print("env variable not set")
        sys.exit()
    driver = GraphDatabase.driver(
        f"bolt://{external_ip}",
        auth=("neo4j", neo4j_pw)
    )

    driver.session().execute_write(import_csv)

if __name__ == "__main__":
    main()