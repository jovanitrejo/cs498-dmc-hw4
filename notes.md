# Issues Encountered
Could not install Java 21 using
```commandline
# apt-get -y temurin-21-jdk
```
so instead, I ran
```commandline
# apt-get -y openjdk-21-jdk
```
Which is another version of Java version 21.

---

To import the CSV file, the uploaded `taxi_trips_clean.csv` was uploaded to the GCP instance. Avoided the use of complex Pandas DF manipulation.

---

Had to also create a separate firewall rule in GCP that allowed port 8000 to any external client.