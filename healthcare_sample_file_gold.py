# Step 8 - Create Gold Layer tables with liquid clustering

# Patient summary table
spark.sql("""
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.gold.patient_summary (
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
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.gold.diagnosis_analytics (
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
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.gold.facility_performance (
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
CREATE TABLE IF NOT EXISTS pwc_aidp_datalake.gold.patient_readmission_features (
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
silver_df = spark.table("pwc_aidp_datalake.silver.patient_diagnoses_clean")

# Create patient summary aggregates
# FIX: count()/countDistinct() return BIGINT, but the table columns are INT.
#      Cast every count-based column to INT so it matches the table schema
#      (resolves DELTA_FAILED_TO_MERGE_FIELDS on 'total_diagnoses', etc.).
patient_summary_df = silver_df.groupBy("patient_id").agg(
    count("*").cast("int").alias("total_diagnoses"),
    countDistinct("diagnosis_code").cast("int").alias("unique_diagnoses"),
    min("diagnosis_date").alias("first_diagnosis_date"),
    max("diagnosis_date").alias("last_diagnosis_date"),
    datediff(max("diagnosis_date"), min("diagnosis_date")).cast("int").alias("patient_tenure_days"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("avg_severity_score"),
    countDistinct("facility_id").cast("int").alias("facilities_used"),
    countDistinct("treating_physician").cast("int").alias("physicians_seen"),
    countDistinct(date_format("diagnosis_date", "yyyy-MM")).cast("int").alias("active_months"),
    (avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ) > 0.6).alias("high_severity_flag"),
    (countDistinct("diagnosis_code") > 4).alias("complex_case_flag"),
    current_timestamp().alias("last_update_timestamp")
).orderBy("patient_id")

# Insert into gold layer
patient_summary_df.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.gold.patient_summary")

print(f"Created patient summaries for {patient_summary_df.count()} patients")
print("Patient summary provides consolidated view of patient pwc_aidp_datalake journey.")

# Step 10 - Populate Gold Layer: Diagnosis Analytics

# Create diagnosis analytics by code and month
# FIX: cast count-based columns to INT to match the table schema.
diagnosis_analytics_df = silver_df.withColumn("month", date_format("diagnosis_date", "yyyy-MM"))
diagnosis_analytics_df = diagnosis_analytics_df.groupBy("diagnosis_code", "diagnosis_description", "month").agg(
    count("*").cast("int").alias("diagnosis_count"),
    countDistinct("patient_id").cast("int").alias("unique_patients"),
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
    countDistinct("facility_id").cast("int").alias("facility_count"),
    countDistinct("treating_physician").cast("int").alias("physician_count")
).orderBy("diagnosis_code", "month")

# Insert into gold layer
diagnosis_analytics_df.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.gold.diagnosis_analytics")

print(f"Created diagnosis analytics for {diagnosis_analytics_df.count()} diagnosis-month combinations")
print("Diagnosis analytics enables pwc_aidp_datalake trend analysis and resource planning.")

# Step 11 - Populate Gold Layer: Facility Performance

# Create facility performance metrics by facility and month
# FIX: cast count-based columns (incl. the sum) to INT to match the table schema.
facility_df = silver_df.withColumn("month", date_format("diagnosis_date", "yyyy-MM"))
facility_performance_df = facility_df.groupBy("facility_id", "month").agg(
    count("*").cast("int").alias("total_diagnoses"),
    countDistinct("patient_id").cast("int").alias("unique_patients"),
    countDistinct("treating_physician").cast("int").alias("unique_physicians"),
    round(avg(
        when(col("severity_level") == "Critical", 1.0)
        .when(col("severity_level") == "High", 0.75)
        .when(col("severity_level") == "Medium", 0.5)
        .otherwise(0.25)
    ), 3).alias("avg_severity_score"),
    sum(when(col("severity_level") == "Critical", 1).otherwise(0)).cast("int").alias("critical_case_count"),
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
facility_performance_df.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.gold.facility_performance")

print(f"Created facility performance metrics for {facility_performance_df.count()} facility-month combinations")
print("Facility performance enables operational analytics and quality monitoring.")

# Step 12 - Populate Gold Layer: ML-Ready Readmission Features

# Use pure SQL to avoid window function issues - create temporary views first
silver_df.createOrReplaceTempView("silver_diagnoses")

# Calculate patient-level features using SQL with proper subquery structure
# FIX 1: CAST the count-based columns to INT to match the table schema.
# FIX 2: the flags high_visit_frequency / complex_case / high_severity_patient
#        are declared BOOLEAN in the table, so emit boolean expressions
#        (x > n) instead of CASE WHEN ... THEN 1 ELSE 0 (which is INT).
ml_features_sql = """
SELECT 
    p.patient_id,
    CAST(p.total_diagnoses AS INT) as total_diagnoses,
    CAST(p.unique_diagnoses AS INT) as unique_diagnoses,
    ROUND(p.avg_severity_score, 3) as avg_severity_score,
    CAST(p.facilities_used AS INT) as facilities_used,
    CAST(p.physicians_seen AS INT) as physicians_seen,
    CAST(p.active_months AS INT) as active_months,
    CAST(p.days_since_last_visit AS INT) as days_since_last_visit,
    CAST(p.patient_tenure_days AS INT) as patient_tenure_days,
    COALESCE(v.avg_days_between_visits, 30.0) as avg_days_between_visits,
    (p.total_diagnoses > 6) as high_visit_frequency,
    (p.unique_diagnoses > 4) as complex_case,
    (p.avg_severity_score > 0.6) as high_severity_patient,
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
        -- FIX: in raw Spark SQL, literals like 1.0 are DECIMAL, so AVG(...)
        --      returns DECIMAL and clashes with the DOUBLE table column
        --      (DELTA_FAILED_TO_MERGE_FIELDS on 'avg_severity_score').
        --      Cast the average to DOUBLE to match the table schema.
        CAST(AVG(CASE 
            WHEN severity_level = 'Critical' THEN 1.0
            WHEN severity_level = 'High' THEN 0.75
            WHEN severity_level = 'Medium' THEN 0.5
            ELSE 0.25
        END) AS DOUBLE) as avg_severity_score,
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
ml_features_df.write.mode("overwrite").saveAsTable("pwc_aidp_datalake.gold.patient_readmission_features")

print(f"Created ML-ready features for {ml_features_df.count()} patients")
print("ML features enable predictive analytics for patient readmission risk.")

# Step 13 - Demonstrate Gold Layer analytics capabilities

print("=== Gold Layer Analytics Demonstration ===")

# Patient summary analytics
print("\nPatient Summary Analytics:")
patient_summary = spark.table("pwc_aidp_datalake.gold.patient_summary")
patient_summary.select(
    "patient_id", "total_diagnoses", "unique_diagnoses", 
    "avg_severity_score", "high_severity_flag", "complex_case_flag"
).show(5)

# Diagnosis analytics
print("\nDiagnosis Analytics:")
diagnosis_analytics = spark.table("pwc_aidp_datalake.gold.diagnosis_analytics")
diagnosis_analytics.select(
    "diagnosis_code", "month", "diagnosis_count", 
    "unique_patients", "critical_case_ratio"
).orderBy(desc("diagnosis_count")).show(5)

# Facility performance
print("\nFacility Performance:")
facility_performance = spark.table("pwc_aidp_datalake.gold.facility_performance")
facility_performance.select(
    "facility_id", "month", "total_diagnoses", 
    "avg_severity_score", "efficiency_score"
).orderBy(desc("total_diagnoses")).show(5)

# ML features preview
print("\nML Features Preview:")
ml_features = spark.table("pwc_aidp_datalake.gold.patient_readmission_features")
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
ml_features_df = spark.table("pwc_aidp_datalake.gold.patient_readmission_features")

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