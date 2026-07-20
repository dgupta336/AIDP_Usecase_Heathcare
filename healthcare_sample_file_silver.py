# Step 5 - Create Silver Layer Delta table with liquid clustering. Test DG

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

# --- FIX ---
# Select ONLY the columns defined in the silver table, in the same order.
# withColumn() keeps every column that comes from bronze, so ingestion_timestamp
# was surviving into silver_df and causing the Delta schema-mismatch error
# (_LEGACY_ERROR_TEMP_DELTA_0007). Selecting the exact target columns drops it
# and guarantees the DataFrame aligns with the table schema.
silver_df = silver_df.select(
    "patient_id",
    "diagnosis_date",
    "diagnosis_code",
    "diagnosis_description",
    "severity_level",
    "treating_physician",
    "facility_id",
    "is_valid_record",
    "data_quality_score",
    "processing_timestamp"
)

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