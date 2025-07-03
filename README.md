# 🚗 Electric Vehicle Specs - PySpark Practice Dataset

Welcome to the **Electric Vehicle Specifications PySpark Practice Project**! This repository is designed to help learners and professionals practice and improve their **PySpark** skills using a real-world structured dataset of electric vehicles (EVs).

This dataset covers various attributes of EVs like range, top speed, battery capacity, drivetrain, and more.

---

## 📁 Repository Structure

```
├── Questions.md           # 50 practice questions (35 Easy + 15 Medium)
├── Solutions.md           # PySpark solutions to all questions
├── electric_vehicles_spec_2025.csv.csv  # Main dataset
├── My_Solutions.pdf       # pdf version of My_Solutions.ipynb
├── My_Solutions.ipynb     # Jupyter notebook with My Solutions
├── README.md              # Project documentation
```

---

## 🔍 Dataset Overview

The dataset includes the following attributes for each vehicle:

- `brand`, `model`, `segment`, `drivetrain`, `car_body_type`
- `battery_capacity_kWh`, `number_of_cells`, `battery_type`
- `top_speed_kmh`, `range_km`, `efficiency_wh_per_km`
- `acceleration_0_100_s`, `fast_charging_power_kw_dc`, `fast_charge_port`
- `cargo_volume_l`, `seats`, `towing_capacity_kg`
- `length_mm`, `width_mm`, `height_mm`
- `source_url` (for reference)

---

## 🧠 Learning Goals

This project is ideal for:

- Practicing **DataFrame operations** (select, filter, groupBy, joins, etc.)
- Learning about **data cleaning** (null handling, casting)
- Understanding **window functions**, **UDFs**, **aggregations**
- Exploring real-world structured data in a Spark context

---

## 📌 Practice Set

- **🔹 Easy (35 Questions)**

  - Basics: Schema, filtering, selecting, sorting
  - Null handling, renaming, casting, simple UDFs
  - Aggregations and distinct counts

- **🟠 Medium (15 Questions)**

  - Window functions, pivot/unpivot
  - Multi-condition filters
  - Advanced UDFs and performance tricks (cache, explode)
  - Join with external metadata (e.g., brand origin)

---

## 💻 Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/shivamdubey2310/pySpark_practice.git
cd Set_1
```

### 2. Install Dependencies

Make sure you have [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) installed:

```bash
pip install pyspark
```

### 3. Launch Notebook

Open the notebook and start practicing:

```bash
jupyter notebook pyspark_practice.ipynb
```

---


## Credits

Created by [Shivam Dubey] — [LinkedIn](www.linkedin.com/in/shivam-dubey20)

If you found this useful, please ⭐ the repo and share it with others in the data community!

---

Happy Learning! 🚀

