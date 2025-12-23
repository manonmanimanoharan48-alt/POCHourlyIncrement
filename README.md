**Explored on Data validation frameworks using Great Expectations (GX).**
 
1. Foundations -Understand Data Quality Concepts.
2. Setup Great Expectations (GX)
3. Core GX Concepts
4. Integrate GX in Data Engineering Environments
5. Data Validation with Checkpoints
6. Add GX to ETL / ELT Pipelines
7. Generate Data Docs (Validation Reports)
 
**Project Example: GX with Fabric Lakehouse**
Integrated great expectation with "Implementing incremental aggregation process in Pyspark notebook using global API data" task.

**Reference:**
End-to-end data validation strategies in Microsoft Fabric.
https://www.youtube.com/watch?v=wAayC-J9TsU


**Implementing incremental aggregation process in Pyspark notebook using global API data**.
1. Fetch live weather data every 1hr using free OpenWeatherMap API(https://openweathermap.org/api)
2. Get data in JSON format from API and store raw data into table format.
3. Use the PySpark notebook to create hourly aggregates
	-Groups all data into 1-hour windows by city.
4. Apply aggregation on grouped data and save data into table.
5. Schedule this notebook every hour.
