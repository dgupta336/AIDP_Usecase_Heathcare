# Dhaval's code
# Added this line in Git
# Step 1 - Create pwc_aidp_datalake catalog and medallion schemas

spark.sql("CREATE CATALOG IF NOT EXISTS pwc_aidp_datalake")

spark.sql("CREATE SCHEMA IF NOT EXISTS pwc_aidp_datalake.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS pwc_aidp_datalake.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS pwc_aidp_datalake.gold")

print("pwc_aidp_datalake catalog and medallion schemas (bronze, silver, gold) created successfully!")

# Step 2 - Create Bronze Layer Delta table with liquid clustering

spark.sql("""
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.bronze.patient_diagnoses_raw (
    patient_id STRING,
    diagnosis_date STRING,  -- Raw date string, various formats possible
    diagnosis_code STRING,
    diagnosis_description STRING,
    severity_level STRING,
    treating_physician STRING,
    facility_id STRING,
    ingestion_timestamp TIMESTAMP  -- When data was ingested
)
USING DELTA
CLUSTER BY (ingestion_timestamp, patient_id)
""")

print("Bronze layer table created successfully!")
print("Clustering will optimize for time-based ingestion and patient-centric queries.")

# Step 3  
import random
from datetime import datetime, timedelta

# Define pwc_aidp_datalake data with some inconsistencies (bronze layer characteristics)
DIAGNOSES_RAW = [
    ("E11.9", "Type 2 diabetes mellitus without complications", "Medium"),
    ("e11.9", "type 2 diabetes mellitus without complications", "medium"),  # Case inconsistency
    ("I10", "Essential hypertension", "High"),
    ("i10", "essential hypertension", "high"),  # Case inconsistency
    ("J45.909", "Unspecified asthma, uncomplicated", "Medium"),
    ("M54.5", "Low back pain", "Low"),
    ("N39.0", "Urinary tract infection, site not specified", "Medium"),
    ("Z51.11", "Encounter for antineoplastic chemotherapy", "Critical"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery without angina pectoris", "High"),
    ("F41.9", "Anxiety disorder, unspecified", "Medium"),
    ("", "", ""),  # Some missing values
    ("INVALID", "Invalid diagnosis", "Unknown")  # Invalid data
]

FACILITIES_RAW = ["HOSP001", "hosp002", "CLINIC001", "clinic002", "URGENT001", ""]  # Case and missing
PHYSICIANS_RAW = ["DR_SMITH", "dr_johnson", "DR_WILLIAMS", "dr_brown", "DR_JONES", "dr_garcia", "", "UNKNOWN"]

# Different date formats to simulate raw data
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y", "%Y/%m/%d"]

# Generate raw patient diagnosis records
raw_patient_data = []
base_date = datetime(2024, 1, 1)
ingestion_time = datetime.now()

# Create 1,000 patients with 2-8 diagnoses each, including some data quality issues
for patient_num in range(1, 1001):
    patient_id = f"PAT{patient_num:04d}"
    
    # Each patient gets 2-8 diagnoses over 12 months
    num_diagnoses = random.randint(2, 8)
    
    for i in range(num_diagnoses):
        # Spread diagnoses over 12 months
        days_offset = random.randint(0, 365)
        diagnosis_date_obj = base_date + timedelta(days=days_offset)
        
        # Random date format to simulate raw data inconsistency
        date_format = random.choice(DATE_FORMATS)
        diagnosis_date_str = diagnosis_date_obj.strftime(date_format)
        
        # Select random diagnosis (including some with quality issues)
        diagnosis_code, description, severity = random.choice(DIAGNOSES_RAW)
        
        # Select random facility and physician (including inconsistencies)
        facility = random.choice(FACILITIES_RAW)
        physician = random.choice(PHYSICIANS_RAW)
        
        # Occasionally introduce missing values (bronze layer realism)
        if random.random() < 0.05:  # 5% chance of missing data
            diagnosis_code = None if random.random() < 0.5 else diagnosis_code
            severity = None if random.random() < 0.5 else severity
        
        raw_patient_data.append({
            "patient_id": patient_id,
            "diagnosis_date": diagnosis_date_str,
            "diagnosis_code": diagnosis_code,
            "diagnosis_description": description,
            "severity_level": severity,
            "treating_physician": physician,
            "facility_id": facility,
            "ingestion_timestamp": ingestion_time
        })

print(f"Generated {len(raw_patient_data)} raw patient diagnosis records")
print("Raw data includes formatting inconsistencies, missing values, and data quality issues")
print("Sample raw record:", raw_patient_data[0])

# Step 4 - Insert raw data into Bronze layer

# Create DataFrame from raw generated data
df_bronze = spark.createDataFrame(raw_patient_data)

# Display schema and sample data
print("Bronze Layer DataFrame Schema:")
df_bronze.printSchema()

print("\nSample Raw Data:")
df_bronze.show(5)

# Insert data into Bronze Delta table
df_bronze.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.bronze.patient_diagnoses_raw")

print(f"\nSuccessfully inserted {df_bronze.count()} raw records into pwc_aidp_datalake.bronze.patient_diagnoses_raw")
print("Bronze layer preserves raw data as-is for auditability and reprocessing.")

# Step 5 - Create Silver Layer Delta table with liquid clustering

spark.sql("""
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.silver.patient_diagnoses_clean (
    patient_id STRING,
    diagnosis_date DATE,
    diagnosis_code STRING,
    diagnosis_description STRING,
    severity_level STRING,
    treating_physician STRING,
    facility_id STRING,
    is_valid_record BOOLEAN,
    data_quality_score DOUBLE,
    processing_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (patient_id, diagnosis_date)
""")

print("Silver layer table created successfully!")
print("Clustering optimizes for patient-centric and time-based analytics.")

# Step 6 - Transform bronze data to silver layer with cleaning and standardization

from pyspark.sql.functions import *
from pyspark.sql.types import DateType

# Read bronze data
bronze_df = spark.table("pwc_aidp_datalake.bronze.patient_diagnoses_raw")

# Data cleaning and standardization transformations
silver_df = bronze_df.withColumn(
    "diagnosis_date",
    coalesce(
        to_date("diagnosis_date", "yyyy-MM-dd"),
        to_date("diagnosis_date", "MM/dd/yyyy"),
        to_date("diagnosis_date", "dd-MMM-yyyy"),
        to_date("diagnosis_date", "yyyy/MM/dd")
    )
).withColumn(
    "diagnosis_code",
    upper(trim("diagnosis_code"))
).withColumn(
    "diagnosis_description",
    initcap(trim("diagnosis_description"))
).withColumn(
    "severity_level",
    when(upper(trim("severity_level")).isin(["CRITICAL", "HIGH", "MEDIUM", "LOW"]), 
         initcap(trim("severity_level")))
    .otherwise("Unknown")
).withColumn(
    "treating_physician",
    when(trim("treating_physician") != "", upper(trim("treating_physician")))
    .otherwise("UNKNOWN")
).withColumn(
    "facility_id",
    when(trim("facility_id") != "", upper(trim("facility_id")))
    .otherwise("UNKNOWN")
).withColumn(
    "is_valid_record",
    (col("patient_id").isNotNull()) & 
    (col("diagnosis_date").isNotNull()) & 
    (col("diagnosis_code").isNotNull()) &
    (length(trim("diagnosis_code")) > 0)
).withColumn(
    "data_quality_score",
    (when(col("patient_id").isNotNull(), 0.2).otherwise(0) +
     when(col("diagnosis_date").isNotNull(), 0.2).otherwise(0) +
     when(col("diagnosis_code").isNotNull() & (length(trim("diagnosis_code")) > 0), 0.2).otherwise(0) +
     when(col("severity_level") != "Unknown", 0.2).otherwise(0) +
     when(col("treating_physician") != "UNKNOWN", 0.2).otherwise(0))
).withColumn(
    "processing_timestamp",
    current_timestamp()
).filter(
    col("is_valid_record") == True  # Only keep valid records in silver layer
)

# Remove duplicates based on patient_id, diagnosis_date, diagnosis_code
silver_df = silver_df.dropDuplicates(["patient_id", "diagnosis_date", "diagnosis_code"])

# Insert cleaned data into silver layer
silver_df.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.silver.patient_diagnoses_clean")

print(f"Successfully processed {silver_df.count()} clean records into pwc_aidp_datalake.silver.patient_diagnoses_clean")
print("Silver layer provides standardized, validated, and enriched data for analytics.")

# Step 7 - Validate silver layer data quality improvements

print("=== Silver Layer Data Quality Validation ===")

# Compare bronze vs silver data quality
bronze_count = spark.table("pwc_aidp_datalake.bronze.patient_diagnoses_raw").count()
silver_count = spark.table("pwc_aidp_datalake.silver.patient_diagnoses_clean").count()

print(f"Bronze layer records: {bronze_count}")
print(f"Silver layer records: {silver_count}")
print(f"Data quality improvement: {((silver_count/bronze_count)*100):.1f}% valid records retained")

# Show data quality distribution
quality_distribution = spark.table("pwc_aidp_datalake.silver.patient_diagnoses_clean").groupBy("data_quality_score").count().orderBy("data_quality_score")
quality_distribution.show()

# Sample cleaned records
print("\nSample Cleaned Records:")
spark.table("pwc_aidp_datalake.silver.patient_diagnoses_clean").show(5)

# Step 8 - Create Gold Layer tables with liquid clustering

# Patient summary table
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.patient_summary (
    patient_id STRING,
    total_diagnoses INT,
    unique_diagnoses INT,
    first_diagnosis_date DATE,
    last_diagnosis_date DATE,
    patient_tenure_days INT,
    avg_severity_score DOUBLE,
    facilities_used INT,
    physicians_seen INT,
    active_months INT,
    high_severity_flag BOOLEAN,
    complex_case_flag BOOLEAN,
    last_update_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (patient_id)
""")

# Diagnosis analytics table
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.diagnosis_analytics (
    diagnosis_code STRING,
    diagnosis_description STRING,
    month STRING,
    diagnosis_count INT,
    unique_patients INT,
    avg_severity_score DOUBLE,
    critical_case_ratio DOUBLE,
    facility_count INT,
    physician_count INT
)
USING DELTA
CLUSTER BY (diagnosis_code, month)
""")

# Facility performance table
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.facility_performance (
    facility_id STRING,
    month STRING,
    total_diagnoses INT,
    unique_patients INT,
    unique_physicians INT,
    avg_severity_score DOUBLE,
    critical_case_count INT,
    patient_satisfaction_proxy DOUBLE,
    efficiency_score DOUBLE
)
USING DELTA
CLUSTER BY (facility_id, month)
""")

# ML-ready readmission features table
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.patient_readmission_features (
    patient_id STRING,
    total_diagnoses INT,
    unique_diagnoses INT,
    avg_severity_score DOUBLE,
    facilities_used INT,
    physicians_seen INT,
    active_months INT,
    days_since_last_visit INT,
    patient_tenure_days INT,
    avg_days_between_visits DOUBLE,
    high_visit_frequency BOOLEAN,
    complex_case BOOLEAN,
    high_severity_patient BOOLEAN,
    readmission_risk_label INT,
    feature_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (patient_id)
""")

print("Gold layer tables created successfully!")
print("Each table is optimized with liquid clustering for specific query patterns.")

# Step 9 - Populate Gold Layer: Patient Summary

from pyspark.sql.functions import *

# Read silver layer data
silver_df = spark.table("healthcare.silver.patient_diagnoses_clean")

# Create patient summary aggregates
patient_summary_df = silver_df.groupBy("patient_id").agg(
    count("*").alias("total_diagnoses"),
    countDistinct("diagnosis_code").alias("unique_diagnoses"),
    min("diagnosis_date").alias("first_diagnosis_date"),
    max("diagnosis_date").alias("last_diagnosis_date"),
    datediff(max("diagnosis_date"), min("diagnosis_date")).alias("patient_tenure_days"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("avg_severity_score"),
    countDistinct("facility_id").alias("facilities_used"),
    countDistinct("treating_physician").alias("physicians_seen"),
    countDistinct(date_format("diagnosis_date", "yyyy-MM")).alias("active_months"),
    (avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ) > 0.6).alias("high_severity_flag"),
    (countDistinct("diagnosis_code") > 4).alias("complex_case_flag"),
    current_timestamp().alias("last_update_timestamp")
).orderBy("patient_id")

# Insert into gold layer
patient_summary_df.write.mode("overwrite").saveAsTable("healthcare.gold.patient_summary")

print(f"Created patient summaries for {patient_summary_df.count()} patients")
print("Patient summary provides consolidated view of patient healthcare journey.")

# Step 10 - Populate Gold Layer: Diagnosis Analytics

# Create diagnosis analytics by code and month
diagnosis_analytics_df = silver_df.withColumn("month", date_format("diagnosis_date", "yyyy-MM"))
diagnosis_analytics_df = diagnosis_analytics_df.groupBy("diagnosis_code", "diagnosis_description", "month").agg(
    count("*").alias("diagnosis_count"),
    countDistinct("patient_id").alias("unique_patients"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("avg_severity_score"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .otherwise(0.0)
    ), 3).alias("critical_case_ratio"),
    countDistinct("facility_id").alias("facility_count"),
    countDistinct("treating_physician").alias("physician_count")
).orderBy("diagnosis_code", "month")

# Insert into gold layer
diagnosis_analytics_df.write.mode("overwrite").saveAsTable("healthcare.gold.diagnosis_analytics")

print(f"Created diagnosis analytics for {diagnosis_analytics_df.count()} diagnosis-month combinations")
print("Diagnosis analytics enables healthcare trend analysis and resource planning.")

# Step 11 - Populate Gold Layer: Facility Performance

# Create facility performance metrics by facility and month
facility_df = silver_df.withColumn("month", date_format("diagnosis_date", "yyyy-MM"))
facility_performance_df = facility_df.groupBy("facility_id", "month").agg(
    count("*").alias("total_diagnoses"),
    countDistinct("patient_id").alias("unique_patients"),
    countDistinct("treating_physician").alias("unique_physicians"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("avg_severity_score"),
    sum(when(col("severity_level") == "Critical", 1).otherwise(0)).alias("critical_case_count"),
    # Patient satisfaction proxy (inverse of severity and case complexity)
    round(1.0 - avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("patient_satisfaction_proxy"),
    # Efficiency score (diagnoses per physician)
    round(count("*") / countDistinct("treating_physician"), 2).alias("efficiency_score")
).orderBy("facility_id", "month")

# Insert into gold layer
facility_performance_df.write.mode("overwrite").saveAsTable("healthcare.gold.facility_performance")

print(f"Created facility performance metrics for {facility_performance_df.count()} facility-month combinations")
print("Facility performance enables operational analytics and quality monitoring.")

# Step 12 - Populate Gold Layer: ML-Ready Readmission Features

# Use pure SQL to avoid window function issues - create temporary views first
silver_df.createOrReplaceTempView("silver_diagnoses")

# Calculate patient-level features using SQL with proper subquery structure
ml_features_sql = """
SELECT 
    p.patient_id,
    p.total_diagnoses,
    p.unique_diagnoses,
    ROUND(p.avg_severity_score, 3) as avg_severity_score,
    p.facilities_used,
    p.physicians_seen,
    p.active_months,
    p.days_since_last_visit,
    p.patient_tenure_days,
    COALESCE(v.avg_days_between_visits, 30) as avg_days_between_visits,
    CASE WHEN p.total_diagnoses > 6 THEN 1 ELSE 0 END as high_visit_frequency,
    CASE WHEN p.unique_diagnoses > 4 THEN 1 ELSE 0 END as complex_case,
    CASE WHEN p.avg_severity_score > 0.6 THEN 1 ELSE 0 END as high_severity_patient,
    CASE WHEN 
        p.total_diagnoses > 6 OR 
        p.unique_diagnoses > 4 OR 
        p.avg_severity_score > 0.6 OR
        p.facilities_used > 2
    THEN 1 ELSE 0 END as readmission_risk_label,
    CURRENT_TIMESTAMP() as feature_timestamp
FROM (
    -- Patient-level aggregates
    SELECT 
        patient_id,
        COUNT(*) as total_diagnoses,
        COUNT(DISTINCT diagnosis_code) as unique_diagnoses,
        AVG(CASE 
            WHEN severity_level = 'Critical' THEN 1.0
            WHEN severity_level = 'High' THEN 0.75
            WHEN severity_level = 'Medium' THEN 0.5
            ELSE 0.25
        END) as avg_severity_score,
        COUNT(DISTINCT facility_id) as facilities_used,
        COUNT(DISTINCT treating_physician) as physicians_seen,
        COUNT(DISTINCT DATE_FORMAT(diagnosis_date, 'yyyy-MM')) as active_months,
        DATEDIFF(CURRENT_DATE(), MAX(diagnosis_date)) as days_since_last_visit,
        DATEDIFF(MAX(diagnosis_date), MIN(diagnosis_date)) as patient_tenure_days
    FROM silver_diagnoses
    GROUP BY patient_id
) p
LEFT JOIN (
    -- Average days between visits
    SELECT 
        patient_id,
        ROUND(AVG(days_between_visits), 2) as avg_days_between_visits
    FROM (
        SELECT 
            patient_id,
            diagnosis_date,
            LAG(diagnosis_date) OVER (PARTITION BY patient_id ORDER BY diagnosis_date) as prev_date,
            DATEDIFF(diagnosis_date, LAG(diagnosis_date) OVER (PARTITION BY patient_id ORDER BY diagnosis_date)) as days_between_visits
        FROM silver_diagnoses
    ) 
    WHERE days_between_visits IS NOT NULL
    GROUP BY patient_id
) v ON p.patient_id = v.patient_id
"""

# Execute the SQL query
ml_features_df = spark.sql(ml_features_sql)

# Insert into gold layer
ml_features_df.write.mode("overwrite").saveAsTable("healthcare.gold.patient_readmission_features")

print(f"Created ML-ready features for {ml_features_df.count()} patients")
print("ML features enable predictive analytics for patient readmission risk.")

# Step 13 - Demonstrate Gold Layer analytics capabilities

print("=== Gold Layer Analytics Demonstration ===")

# Patient summary analytics
print("\nPatient Summary Analytics:")
patient_summary = spark.table("healthcare.gold.patient_summary")
patient_summary.select(
    "patient_id", "total_diagnoses", "unique_diagnoses", 
    "avg_severity_score", "high_severity_flag", "complex_case_flag"
).show(5)

# Diagnosis analytics
print("\nDiagnosis Analytics:")
diagnosis_analytics = spark.table("healthcare.gold.diagnosis_analytics")
diagnosis_analytics.select(
    "diagnosis_code", "month", "diagnosis_count", 
    "unique_patients", "critical_case_ratio"
).orderBy(desc("diagnosis_count")).show(5)

# Facility performance
print("\nFacility Performance:")
facility_performance = spark.table("healthcare.gold.facility_performance")
facility_performance.select(
    "facility_id", "month", "total_diagnoses", 
    "avg_severity_score", "efficiency_score"
).orderBy(desc("total_diagnoses")).show(5)

# ML features preview
print("\nML Features Preview:")
ml_features = spark.table("healthcare.gold.patient_readmission_features")
ml_features.select(
    "patient_id", "total_diagnoses", "avg_severity_score", 
    "readmission_risk_label"
).show(5)

# Step 14 - Train patient readmission prediction model using gold layer features

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Read ML-ready features from gold layer
ml_features_df = spark.table("healthcare.gold.patient_readmission_features")

# Prepare features for model training
feature_cols = [
    "total_diagnoses", "unique_diagnoses", "avg_severity_score", 
    "facilities_used", "physicians_seen", "active_months", 
    "days_since_last_visit", "patient_tenure_days", "avg_days_between_visits",
    "high_visit_frequency", "complex_case", "high_severity_patient"
]

# Assemble features
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Create Random Forest model
rf = RandomForestClassifier(
    labelCol="readmission_risk_label", 
    featuresCol="scaled_features",
    numTrees=100,
    maxDepth=10,
    seed=42
)

# Create pipeline
pipeline = Pipeline(stages=[assembler, scaler, rf])

# Split data
train_data, test_data = ml_features_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_data.count()} patients")
print(f"Test set: {test_data.count()} patients")

# Step 15 - Train the model

print("Training patient readmission prediction model...")
model = pipeline.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate model
evaluator = BinaryClassificationEvaluator(labelCol="readmission_risk_label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

print(f"Model AUC: {auc:.4f}")

# Show predictions
predictions.select(
    "patient_id", "total_diagnoses", "avg_severity_score", 
    "readmission_risk_label", "prediction", "probability"
).show(10)

# Confusion matrix
confusion_matrix = predictions.groupBy("readmission_risk_label", "prediction").count()
confusion_matrix.show()

# Step 16 - Model interpretation and business impact analysis

# Feature importance
rf_model = model.stages[-1]
feature_importance = rf_model.featureImportances

print("=== Feature Importance for Readmission Prediction ===")
for name, importance in zip(feature_cols, feature_importance):
    print(f"{name}: {importance:.4f}")

print("\n=== Business Impact Analysis ===")

# Calculate potential impact
high_risk_predictions = predictions.filter("prediction = 1")
patients_at_risk = high_risk_predictions.count()
total_test_patients = test_data.count()

print(f"Total test patients: {total_test_patients}")
print(f"Patients predicted as high readmission risk: {patients_at_risk}")
print(f"Percentage flagged for intervention: {(patients_at_risk/total_test_patients)*100:.1f}%")

# Cost savings potential
avg_readmission_cost = 15000
intervention_success_rate = 0.3
avg_intervention_cost = 2000

prevented_readmissions = patients_at_risk * intervention_success_rate
cost_savings = prevented_readmissions * avg_readmission_cost
total_intervention_cost = patients_at_risk * avg_intervention_cost
net_savings = cost_savings - total_intervention_cost

print(f"\nEstimated cost per readmission: ${avg_readmission_cost:,}")
print(f"Estimated intervention success rate: {intervention_success_rate*100:.0f}%")
print(f"Potential readmissions prevented: {prevented_readmissions:.0f}")
print(f"Potential cost savings: ${cost_savings:,.0f}")
print(f"Total intervention cost: ${total_intervention_cost:,.0f}")
print(f"Net savings: ${net_savings:,.0f}")

# Model performance metrics
accuracy = predictions.filter("readmission_risk_label = prediction").count() / predictions.count()
precision = predictions.filter("prediction = 1 AND readmission_risk_label = 1").count() / predictions.filter("prediction = 1").count() if predictions.filter("prediction = 1").count() > 0 else 0
recall = predictions.filter("prediction = 1 AND readmission_risk_label = 1").count() / predictions.filter("readmission_risk_label = 1").count() if predictions.filter("readmission_risk_label = 1").count() > 0 else 0

print(f"\nModel Performance:")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"AUC: {auc:.4f}")
