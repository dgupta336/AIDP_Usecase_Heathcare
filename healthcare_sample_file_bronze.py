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
