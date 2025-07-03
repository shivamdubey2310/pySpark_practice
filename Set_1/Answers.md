---

### ✅ Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("EV Spec Practice").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("electric_vehicles_spec_2025.csv")
```

---

## 🟢 Easy-Level Questions (with Answers)

---

### 🔹 DataFrame Basics

**1. Load the CSV file into a DataFrame and print the schema.**

```python
df.printSchema()
```

**2. Display the first 10 rows of the DataFrame.**

```python
df.show(10)
```

**3. Select only `brand`, `model`, and `range_km` columns.**

```python
df.select("brand", "model", "range_km").show()
```

**4. Rename the column `battery_capacity_kWh` to `battery_kWh`.**

```python
df_renamed = df.withColumnRenamed("battery_capacity_kWh", "battery_kWh")
```

**5. Drop the `source_url` column from the DataFrame.**

```python
df_dropped = df.drop("source_url")
```

---

### 🔹 Filtering & Selection

**6. Filter rows where `brand` is "Abarth".**

```python
df.filter(df.brand == "Abarth").show()
```

**7. Find all vehicles with `top_speed_kmh` greater than 180.**

```python
df.filter(df.top_speed_kmh > 180).show()
```

**8. Filter rows where `torque_nm` is not null.**

```python
df.filter(df.torque_nm.isNotNull()).show()
```

**9. Retrieve vehicles where `range_km` is between 250 and 300.**

```python
df.filter((df.range_km >= 250) & (df.range_km <= 300)).show()
```

**10. Filter SUVs with more than 4 seats.**

```python
df.filter((df.car_body_type == "SUV") & (df.seats > 4)).show()
```

---

### 🔹 Aggregations

**11. Average `efficiency_wh_per_km` for each brand.**

```python
df.groupBy("brand").agg(avg("efficiency_wh_per_km")).show()
```

**12. Maximum `top_speed_kmh` for each brand.**

```python
df.groupBy("brand").agg(max("top_speed_kmh")).show()
```

**13. Count how many distinct `car_body_type` values exist.**

```python
df.select("car_body_type").distinct().count()
```

**14. Total number of vehicles for each segment.**

```python
df.groupBy("segment").count().show()
```

**15. Group by `drivetrain` and average `acceleration_0_100_s`.**

```python
df.groupBy("drivetrain").agg(avg("acceleration_0_100_s")).show()
```

---

### 🔹 Sorting

**16. Sort the DataFrame by `range_km` in descending order.**

```python
df.orderBy(df.range_km.desc()).show()
```

**17. Sort by `battery_capacity_kWh`, then by `top_speed_kmh`.**

```python
df.orderBy("battery_capacity_kWh", "top_speed_kmh").show()
```

---

### 🔹 Handling Nulls

**18. Count how many nulls are present in each column.**

```python
from pyspark.sql.functions import col, sum as _sum
df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
```

**19. Drop rows with any null values.**

```python
df.na.drop().show()
```

**20. Fill nulls in `number_of_cells` with 0.**

```python
df.na.fill({"number_of_cells": 0}).show()
```

**21. Fill nulls in `towing_capacity_kg` with the mean value.**

```python
mean_val = df.select(avg("towing_capacity_kg")).first()[0]
df.na.fill({"towing_capacity_kg": mean_val}).show()
```

---

### 🔹 Distinct & Deduplication

**22. Get distinct values from the `brand` column.**

```python
df.select("brand").distinct().show()
```

**23. Drop duplicate rows.**

```python
df.dropDuplicates().show()
```

---

### 🔹 String & Column Operations

**24. Add new column `brand_model` by concatenating brand and model.**

```python
df.withColumn("brand_model", concat_ws(" ", "brand", "model")).show()
```

**25. Extract domain name from the `source_url`.**

```python
df.withColumn("domain", regexp_extract("source_url", r"https?://([^/]+)/", 1)).select("source_url", "domain").show()
```

---

### 🔹 Type Casting

**26. Cast `battery_capacity_kWh` from float to integer.**

```python
df.withColumn("battery_capacity_kWh_int", col("battery_capacity_kWh").cast("int")).show()
```

**27. Convert `range_km` to string.**

```python
df.withColumn("range_km_str", col("range_km").cast("string")).show()
```

---

### 🔹 Basic UDFs

**28. UDF to categorize cars as "High Range" or "Low Range".**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def range_category(r):
    return "High Range" if r and r > 300 else "Low Range"

range_udf = udf(range_category, StringType())
df.withColumn("range_category", range_udf("range_km")).show()
```

**29. UDF to compute `power_density = battery_kWh / length_mm`.**

```python
df.withColumn("power_density", (col("battery_capacity_kWh") / col("length_mm"))).show()
```

---

### 🔹 More Selections

**30. Column `is_fast_charge_supported` where `fast_charging_power_kw_dc` > 50.**

```python
df.withColumn("is_fast_charge_supported", col("fast_charging_power_kw_dc") > 50).show()
```

**31. Filter all rows where `car_body_type` is "SUV".**

```python
df.filter(df.car_body_type == "SUV").show()
```

---

### 🔹 DataFrame Metadata

**32. Show all column names and their data types.**

```python
print(df.dtypes)
```

**33. Count total number of rows in the DataFrame.**

```python
df.count()
```

**34. Check if all entries in `drivetrain` are the same.**

```python
df.select("drivetrain").distinct().show()
```

**35. Count number of vehicles per number of seats.**

```python
df.groupBy("seats").count().show()
```

---
#
#
#

-----

## 🟠 Medium-Level Questions (with Answers)

---

### 🔹 36. For each `brand`, rank vehicles by `range_km` in descending order.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

windowSpec = Window.partitionBy("brand").orderBy(col("range_km").desc())
df.withColumn("range_rank", rank().over(windowSpec)).show()
```

---

### 🔹 37. Calculate the average `battery_capacity_kWh` and standard deviation per `car_body_type`.

```python
df.groupBy("car_body_type").agg(
    avg("battery_capacity_kWh").alias("avg_battery_kWh"),
    stddev("battery_capacity_kWh").alias("stddev_battery_kWh")
).show()
```

---

### 🔹 38. Find the vehicle with the longest `range_km` for each `segment`.

```python
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("segment").orderBy(col("range_km").desc())
df.withColumn("rn", row_number().over(windowSpec)) \
  .filter("rn = 1") \
  .select("segment", "brand", "model", "range_km").show()
```

---

### 🔹 39. Pivot the data to show average `range_km` for each `car_body_type` per `drivetrain`.

```python
df.groupBy("drivetrain") \
  .pivot("car_body_type") \
  .agg(avg("range_km")) \
  .show()
```

---

### 🔹 40. UDF to classify vehicles: "City EV", "Highway EV", or "Performance EV"

```python
def classify_vehicle(accel, range_km):
    if accel is None or range_km is None:
        return "Unknown"
    if accel < 6:
        return "Performance EV"
    elif range_km > 300:
        return "Highway EV"
    else:
        return "City EV"

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

classify_udf = udf(classify_vehicle, StringType())
df.withColumn("ev_type", classify_udf("acceleration_0_100_s", "range_km")).show()
```

---

### 🔹 41. Join with another DataFrame `df_country` having columns `brand` and `country`.

```python
# Sample join DataFrame
from pyspark.sql import Row

df_country = spark.createDataFrame([
    Row(brand="Abarth", country="Italy"),
    Row(brand="Aiways", country="China")
])

df.join(df_country, on="brand", how="left").show()
```

---

### 🔹 42. Find SUVs with top speed > 180 km/h and efficiency < 160 Wh/km.

```python
df.filter((col("car_body_type") == "SUV") &
          (col("top_speed_kmh") > 180) &
          (col("efficiency_wh_per_km") < 160)).show()
```

---

### 🔹 43. Write the DataFrame to JSON with inferred schema.

```python
df.write.mode("overwrite").json("output/ev_data_json")
```

---

### 🔹 44. Infer schema manually using `StructType` and load json using it.

```python
custom_schema = StructType([
    StructField("brand", StringType(), True),
    StructField("model", StringType(), True),
    StructField("top_speed_kmh", IntegerType(), True),
    StructField("battery_capacity_kWh", DoubleType(), True),
    StructField("battery_type", StringType(), True),
    StructField("number_of_cells", DoubleType(), True),
    StructField("torque_nm", DoubleType(), True),
    StructField("efficiency_wh_per_km", IntegerType(), True),
    StructField("range_km", IntegerType(), True),
    StructField("acceleration_0_100_s", DoubleType(), True),
    StructField("fast_charging_power_kw_dc", DoubleType(), True),
    StructField("fast_charge_port", StringType(), True),
    StructField("towing_capacity_kg", DoubleType(), True),
    StructField("cargo_volume_l", IntegerType(), True),
    StructField("seats", IntegerType(), True),
    StructField("drivetrain", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("length_mm", IntegerType(), True),
    StructField("width_mm", IntegerType(), True),
    StructField("height_mm", IntegerType(), True),
    StructField("car_body_type", StringType(), True),
    StructField("source_url", StringType(), True)
])

df_custom = spark.read.schema(custom_schema).json("electric_vehicles_spec_2025.csv")
```

---

### 🔹 45. Unpivot `top_speed_kmh`, `range_km`, `efficiency_wh_per_km`.

```python
from pyspark.sql.functions import posexplode, array, struct, lit

unpivoted = df.select(
    "brand", "model",
    explode(array(
        struct(lit("top_speed_kmh").alias("metric"), col("top_speed_kmh").alias("value")),
        struct(lit("range_km").alias("metric"), col("range_km").alias("value")),
        struct(lit("efficiency_wh_per_km").alias("metric"), col("efficiency_wh_per_km").alias("value"))
    )).alias("kv")
).select("brand", "model", col("kv.metric"), col("kv.value"))

unpivoted.show()
```

---

### 🔹 46. Use RDD to find average `battery_capacity_kWh` per brand.

```python
rdd = df.select("brand", "battery_capacity_kWh").rdd \
        .filter(lambda x: x[1] is not None) \
        .map(lambda x: (x[0], (x[1], 1)))

brand_avg = rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
               .mapValues(lambda x: x[0] / x[1])

brand_avg.collect()
```

---

### 🔹 47. Cache the DataFrame and compare performance (approximation).

```python
import time

start = time.time()
df.groupBy("car_body_type").count().show()
print("Without cache:", time.time() - start)

df.cache()

start = time.time()
df.groupBy("car_body_type").count().show()
print("With cache:", time.time() - start)
```

---

### 🔹 48. Create a struct column `performance` with speed, torque, and acceleration.

```python
df.withColumn("performance", struct("top_speed_kmh", "torque_nm", "acceleration_0_100_s")).select("brand", "model", "performance").show(truncate=False)
```

---

### 🔹 49. Create an array column of all performance metrics and explode it.

```python
df.withColumn("performance_array", array("top_speed_kmh", "torque_nm", "acceleration_0_100_s")) \
  .withColumn("exploded_metric", explode("performance_array")) \
  .select("brand", "model", "exploded_metric").show()
```

---

### 🔹 50. Sort cars by acceleration (asc), then torque (desc), then range.

```python
df.orderBy(
    col("acceleration_0_100_s").asc(),
    col("torque_nm").desc(),
    col("range_km").desc()
).show()
```

---
