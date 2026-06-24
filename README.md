# 🏥 Health Data Pipeline

An end-to-end healthcare data engineering pipeline that extracts data from a relational database, processes it using Apache Spark, stores it in Amazon S3, performs transformations in Hive, and generates analytical reports for healthcare insights.

---

## 📌 Features

- ✅ Extract healthcare data from PostgreSQL database
- ⚡ Process large datasets using Apache Spark (PySpark)
- ☁️ Store raw and processed data in Amazon S3
- 📦 Save data in efficient Parquet format
- 🏥 Create Hive tables for analytics
- 📊 Generate healthcare reports and business insights
- 🔄 Automate workflow using Apache Airflow
- 🚀 Run scalable jobs on Amazon EMR
- 📈 Perform SQL-based healthcare analytics

---

## ⚙️ Tech Stack

| Component | Technology |
|------------|------------|
| Programming Language | Python |
| Data Processing | Apache Spark (PySpark) |
| Workflow Orchestration | Apache Airflow |
| Cloud Storage | Amazon S3 |
| Cluster Computing | Amazon EMR |
| Data Warehouse | Apache Hive |
| Database | PostgreSQL |
| File Format | Parquet |
| Query Language | SQL |

---

## 🧠 How It Works

1. Healthcare data is stored in PostgreSQL.
2. PySpark extracts the data from PostgreSQL.
3. Extracted data is written to Amazon S3 in Parquet format.
4. Data is loaded from S3 into Hive tables.
5. Spark SQL performs transformations and analytics.
6. Reports are generated and saved back to S3.
7. Airflow orchestrates and automates the complete workflow.

---

## 📁 Project Structure
```text
Health-Data-Pipeline/
│
├── health_datapipeline_dag.py
├── rdbms_read_and_write_to_s3.py
├── read_from_s3_and_transform_to_hive.py
├── spark_sql_transformation_and_write_to_s3.py
├── spark_hive_write_to_s3.py
├── read_from_s3_and_transform_to_redshift.py
├── s3_pyspark_hive_sample.py
├── sql_health_ddl.sql
├── insert_health_dml.sql
├── sql_database.png
└── README.md
---

## 🔄 Data Pipeline Flow

```mermaid
flowchart TD
    A[PostgreSQL] --> B[PySpark Extraction]
    B --> C[Amazon S3 (Parquet)]
    C --> D[Apache Hive]
    D --> E[Spark SQL Analytics]
    E --> F[Reports & Insights]
    F --> G[Amazon S3]
```

---

## 🏥 Database Tables

The healthcare database contains the following entities:

| Table | Description |
|--------|------------|
| Patients | Stores patient information |
| Doctors | Stores doctor details and specializations |
| Appointments | Tracks patient-doctor appointments |
| Treatments | Stores treatment information |
| Medications | Stores medication details |
| Prescriptions | Links doctors and medications |
| Billing | Stores patient billing information |
| Medical History | Stores historical health conditions |

---

## 🚀 Getting Started

### 🔧 1. Clone the Repository

```bash
git clone https://github.com/yourusername/Health-Data-Pipeline.git
cd Health-Data-Pipeline
```

### 📦 2. Install Dependencies

```bash
pip install pyspark
pip install apache-airflow
pip install boto3
pip install psycopg2-binary
```

### 🛢 3. Create PostgreSQL Database

Run the DDL script:

```sql
sql_health_ddl.sql
```

Insert sample healthcare data:

```sql
insert_health_dml.sql
```

### ☁️ 4. Configure AWS Services

Configure the following services:

- Amazon S3 Bucket
- Amazon EMR Cluster
- IAM Roles
- Hive Metastore

Example S3 Bucket:

```text
s3://health-data-pipeline/
```

### 🔄 5. Configure Airflow

Copy the DAG file:

```text
health_datapipeline_dag.py
```

to:

```text
airflow/dags/
```

### ▶️ 6. Run the Pipeline

Start Airflow Scheduler:

```bash
airflow scheduler
```

Start Airflow Web Server:

```bash
airflow webserver
```

Then trigger the DAG from the Airflow UI.

---

## 📊 Generated Reports

### 👨‍⚕️ Doctor Performance Report

Contains:

- Number of appointments
- Number of prescriptions
- Treatment statistics

### 👤 Patient Appointment Summary

Contains:

- Appointment details
- Doctor assigned
- Treatment information

### 💰 Billing Analysis Report

Contains:

- Total billing amount
- Average billing
- Highest billing patient
- Revenue statistics

### 🩺 Medical Condition Analysis

Contains:

- Disease frequency
- Patient condition trends
- Healthcare insights

---

## 📈 Sample Analytics

### 💰 Total Billing per Patient

```sql
SELECT PatientID,
       SUM(Amount) AS TotalBilling
FROM Billing
GROUP BY PatientID;
```

### 👨‍⚕️ Appointment Count by Doctor

```sql
SELECT DoctorID,
       COUNT(*) AS TotalAppointments
FROM Appointments
GROUP BY DoctorID;
```

### 💸 High Billing Patients

```sql
SELECT *
FROM Billing
WHERE Amount > 1000;
```

---

## 🧪 Pipeline Components

### 📄 rdbms_read_and_write_to_s3.py

- Reads healthcare data from PostgreSQL
- Converts data into Spark DataFrames
- Writes data to Amazon S3 as Parquet files

### 📄 read_from_s3_and_transform_to_hive.py

- Reads Parquet files from S3
- Performs data cleaning and transformations
- Loads data into Hive tables

### 📄 spark_sql_transformation_and_write_to_s3.py

- Executes Spark SQL queries
- Generates analytical datasets
- Stores results in Amazon S3

### 📄 spark_hive_write_to_s3.py

- Reads Hive tables
- Creates business reports
- Writes reports to Amazon S3

### 📄 health_datapipeline_dag.py

- Apache Airflow DAG
- Automates the entire workflow
- Handles scheduling and task dependencies

---


## 👨‍💻 Author

**Pranali Pawar**  
Computer Engineering Student  
Data Engineering & Analytics Enthusiast

🔗 GitHub: https://github.com/pranalipawar24

---

## 🛡 License

This project is licensed under the **MIT License**.
