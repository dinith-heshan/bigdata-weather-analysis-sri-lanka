# 🌦 Sri Lanka Weather Big Data Analytics & ML Pipeline

## 📌 Project Overview

This project implements a **distributed big data architecture** for large-scale weather data processing and advanced evapotranspiration (ET₀) prediction using **Apache Spark MLlib**.

The system integrates:

* Distributed storage using **HDFS**
* Batch processing using **Hadoop MapReduce**
* SQL-based analytics using **Hive**
* Large-scale processing and ML using **Spark**
* Advanced regression modelling with cross-validation
* Scenario simulation for low evapotranspiration conditions
* Dashboard visualization

The goal is to design a scalable weather analytics platform capable of processing large datasets and generating actionable agricultural insights.

---

# 🏗 System Architecture

The architecture follows a distributed big data design pattern:

### Data Flow

1. Raw weather + location datasets
2. Ingestion into HDFS data lake
3. Data validation via MapReduce
4. Aggregation & querying via Hive
5. Feature engineering & modelling via Spark ML
6. Dashboard visualization layer

### Core Technologies

| Layer                  | Technology             |
| ---------------------- | ---------------------- |
| Storage                | HDFS                   |
| Batch Processing       | Hadoop MapReduce       |
| SQL Analytics          | Hive                   |
| Distributed Processing | Spark                  |
| Machine Learning       | Spark MLlib            |
| Visualization          | Matplotlib / Dashboard |

---

# 📂 Dataset Description

The project uses:

* `locationData.csv`
* `weatherData.csv`

Key features used for modelling:

* Precipitation hours (h)
* Sunshine duration (s)
* Wind speed (km/h)
* ET₀ FAO evapotranspiration (mm) — Target variable

---

# 🧹 Data Processing Pipeline

## 1️⃣ Data Validation (MapReduce)

* Missing value detection
* Schema validation
* Record counting
* Basic aggregations

## 2️⃣ Hive Analytics

* Monthly aggregations
* Location-based summaries
* Statistical exploration

## 3️⃣ Spark Processing

* DataFrame joins
* Null analysis
* Date parsing
* May filtering
* Feature selection
* Duplicate removal
* Aggregation of unique feature combinations

---

# 🤖 Task 3 — Evapotranspiration Prediction (Spark MLlib)

## 🎯 Objective

Predict ET₀ using weather features and identify conditions that result in:

> **Evapotranspiration < 1.5 mm**

---

## 🛠 Feature Engineering

* VectorAssembler for feature vector creation
* StandardScaler for normalization
* Stratified train-test split using 1200 bins
* Distribution preservation verification via histograms

---

## 📊 Models Trained

Four regression models were trained using **5-fold cross-validation**:

### 1️⃣ Generalized Linear Regression (Gaussian)

* Log link
* Tuned `regParam`

### 2️⃣ Generalized Linear Regression (Gamma)

* Log link
* Tuned `regParam`

### 3️⃣ Random Forest Regressor

* Tuned `numTrees`
* Tuned `maxDepth`

### 4️⃣ Gradient Boosted Trees Regressor

* Tuned `maxIter`
* Tuned `maxDepth`

---

## 📏 Evaluation Metrics

Each model was evaluated using:

* RMSE
* MAE
* R²

Evaluation performed on:

* Training dataset
* Test dataset

---

## 📈 Model Diagnostics

The following analyses were performed:

* Predicted vs Actual scatter plots
* Residual vs Actual plots
* Residual distribution histograms
* Train vs Test metric comparison
* Feature importance analysis (tree models)

This ensures:

* Overfitting detection
* Bias detection
* Error distribution understanding
* Interpretability

---

# 🌱 Feature Importance (Tree-Based Models)

Extracted from:

* RandomForestRegressor
* GBTRegressor

Features ranked by contribution:

* Precipitation hours
* Sunshine duration
* Wind speed

This helps understand which variables most strongly influence ET₀.

---

# 🔍 Scenario Simulation & Low-ET Analysis

A comprehensive feature grid was generated across:

* 0–24 precipitation hours
* Full sunshine duration range
* Full wind speed range

Predictions were generated for all combinations.

### Low ET Filter:

```
ET₀ < 1.5 mm
```

For each model:

* Counted qualifying scenarios
* Extracted mean weather conditions
* Validated predicted ET₀ values

This provides **actionable agricultural insight** into low evapotranspiration conditions.

---

# 📊 Task 4 — Dashboard

A dashboard was developed to visualize:

* Weather trends
* Monthly ET₀ variations
* Model predictions
* Comparative performance metrics

📷 Screenshots included in `/dashboard_screenshots/`

---

# 🚀 Key Highlights

* Distributed big data architecture
* Stratified bin-based train/test split
* 5-fold cross-validation
* Hyperparameter tuning
* Multiple regression comparisons
* Residual diagnostics
* Feature importance interpretation
* Scenario-based predictive simulation
* Production-style pipeline design

---

# 📌 Skills Demonstrated

* Big Data Engineering
* Distributed Systems
* Spark MLlib
* Regression Modelling
* Cross-Validation
* Hyperparameter Optimization
* Feature Engineering
* Model Diagnostics
* Data Pipeline Architecture
* Analytical Visualization

---

# 🏁 Conclusion

This project demonstrates the design and implementation of a **scalable weather analytics platform** capable of:

* Processing large datasets in distributed environments
* Building production-grade ML models
* Performing advanced regression diagnostics
* Generating scenario-based predictive insights

It combines **data engineering + big data processing + machine learning + applied analytics** into a unified architecture.
