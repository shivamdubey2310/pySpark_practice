## 🟢 Easy Level (35 Questions)

Focus: RDD/DataFrame basics, filtering, selecting, grouping, sorting, basic aggregations, handling nulls.

### 🔹 DataFrame Basics

1. Load the CSV file into a DataFrame and print the schema.
2. Display the first 10 rows of the DataFrame.
3. Select only `brand`, `model`, and `range_km` columns.
4. Rename the column `battery_capacity_kWh` to `battery_kWh`.
5. Drop the `source_url` column from the DataFrame.

### 🔹 Filtering & Selection

6. Filter rows where `brand` is "Abarth".
7. Find all vehicles with `top_speed_kmh` greater than 180.
8. Filter rows where `torque_nm` is not null.
9. Retrieve vehicles where `range_km` is between 250 and 300.
10. Filter SUVs with more than 4 seats.

### 🔹 Aggregations

11. Calculate the average `efficiency_wh_per_km` for each `brand`.
12. Find the maximum `top_speed_kmh` for each `brand`.
13. Count how many distinct `car_body_type` values exist.
14. Compute the total number of vehicles for each `segment`.
15. Group by `drivetrain` and calculate the average `acceleration_0_100_s`.

### 🔹 Sorting

16. Sort the DataFrame by `range_km` in descending order.
17. Sort vehicles by `battery_capacity_kWh`, then by `top_speed_kmh`.

### 🔹 Handling Nulls

18. Count how many null values are present in each column.
19. Drop rows with any null values.
20. Fill nulls in `number_of_cells` with 0.
21. Fill nulls in `towing_capacity_kg` with the mean value of the column.

### 🔹 Distinct & Deduplication

22. Get distinct values from the `brand` column.
23. Drop duplicate rows from the DataFrame.

### 🔹 String & Column Operations

24. Add a new column `brand_model` by concatenating `brand` and `model`.
25. Extract domain name from the `source_url`.

### 🔹 Type Casting

26. Cast `battery_capacity_kWh` from float to integer.
27. Convert `range_km` to string.

### 🔹 Basic UDFs

28. Create a UDF to categorize cars as "High Range" (>300 km) or "Low Range".
29. Create a UDF to compute `power_density = battery_capacity_kWh / length_mm`.

### 🔹 More Selections

30. Create a column `is_fast_charge_supported` where `fast_charging_power_kw_dc` > 50.
31. Filter all rows where `car_body_type` is "SUV".

### 🔹 DataFrame Metadata

32. Show all column names and their data types.
33. Count total number of rows in the DataFrame.
34. Check if all entries in the `drivetrain` column are the same.
35. Count the number of vehicles per number of seats.

---

## 🟠 Medium Level (15 Questions)

Focus: Window functions, advanced aggregations, joins, UDFs, pivot/unpivot, JSON export, date functions (simulated), performance tuning basics.

### 🔹 Window Functions & Advanced Aggregations

36. For each `brand`, rank vehicles by `range_km` in descending order.
37. Calculate the average `battery_capacity_kWh` and standard deviation per `car_body_type`.
38. Find the vehicle with the longest `range_km` for each `segment`.

### 🔹 Pivoting

39. Pivot the data to show average `range_km` for each `car_body_type` per `drivetrain`.

### 🔹 Advanced UDFs

40. Create a UDF to classify vehicles into: "City EV", "Highway EV", or "Performance EV" based on `acceleration_0_100_s` and `range_km`.

### 🔹 Joins (Synthetic example)

41. Assume you have another DataFrame with `brand` and `country`. Join it with the EV DataFrame on `brand`.

### 🔹 Complex Filtering & Conditions

42. Find SUVs with a top speed over 180 km/h and efficiency under 160 Wh/km.

### 🔹 JSON Export & Schema

43. Write the DataFrame to a JSON file with inferred schema.
44. Infer schema manually using `StructType` and load the json using it.

### 🔹 Unpivot (Melt-like)

45. Transform the column-based format into a long format for `top_speed_kmh`, `range_km`, and `efficiency_wh_per_km`.

### 🔹 Map & ReduceByKey (RDD-style)

46. Convert DataFrame to RDD and find average `battery_capacity_kWh` per brand using `reduceByKey`.

### 🔹 Performance Optimization

47. Cache the DataFrame and perform a groupBy operation. Measure execution time before and after caching.

### 🔹 Nested Column Creation

48. Create a struct column called `performance` with `top_speed_kmh`, `acceleration_0_100_s`, and `torque_nm`.

### 🔹 Exploding Arrays

49. Create an array column of all performance metrics and explode it row-wise.

### 🔹 Complex Sorting

50. Sort cars by acceleration (ascending), then by torque (descending), and then by range.

---
