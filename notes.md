# Issues Encountered
Could not install Java 21 using
```commandline
sudo apt-get -y temurin-21-jdk
```
so instead, I ran
```commandline
sudo apt-get -y openjdk-21-jdk
```
Which is another instance of Java version 21.

---

To import the CSV file, the uploaded `taxi_trips_clean.csv` was uploaded to the GCP instance. Avoided the use of complex Pandas DF manipulation.

---

Had to also create a separate firewall rule in GCP that allowed port 8000 to any external client.

---

Found the following article for rounding in neo4j: [https://dzone.com/articles/neo4j-cypher-rounding-float](Neo4j & Cypher: Rounding a Float Value to Decimal Places)

---

Used the following documentation for using Python Neo4j Driver:
[https://neo4j.com/docs/api/python-driver/current/](https://neo4j.com/docs/api/python-driver/current/)

---

Found the following documentation for part 2: [https://spark.apache.org/docs/latest/sql-getting-started.html](https://spark.apache.org/docs/latest/sql-getting-started.html)