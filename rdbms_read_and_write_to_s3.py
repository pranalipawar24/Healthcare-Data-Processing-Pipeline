from pyspark.sql import SparkSession

# Create a SparkSession
"""
spark = SparkSession.builder \
    .appName("ReadAndWriteToS3") \
    .config("spark.jars", "/path/to/mysql-connector-java.jar") \
    .getOrCreate()

# Database connection parameters
"""
spark = SparkSession.builder.\
appName("ReadAndWriteToS3")\
.master("yarn")\
.enableHiveSupport()\
.getOrCreate()
"""
db_url = "jdbc:mysql://localhost:3306/healthcare"
db_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}
"""
db_url = "jdbc:postgresql://database-1.cniiuy20s4z2.ap-south-1.rds.amazonaws.com:5432/health_db"
db_properties = {
    "user": "postgres",
    "password": "postgres123",
    "driver": "org.postgresql.Driver"
}



# Function to write DataFrame to S3
def write_df_to_s3(df, path, format):
    try:
        if df.count() > 0:
            df.write.mode("overwrite").format(format).save(path)
            print(f"Data successfully written to {path} in {format} format.")
        else:
            print(f"No data found in DataFrame for {path}.")
    except Exception as e:
        print(f"Error writing data to {path} in {format} format: {e}")

# Function to check if DataFrame is not empty
def check_df_not_empty(df, table_name):
    if df.count() > 0:
        print(f"Data found in {table_name} table.")
        return True
    else:
        print(f"No data found in {table_name} table.")
        return False


try:
    # Read data from Patients table
    patients_df = spark.read.jdbc(url=db_url, table="Patients", properties=db_properties)
    if check_df_not_empty(patients_df, "Patients"):
        write_df_to_s3(patients_df, f"s3a://datawarehouse360/healthcare/patients", "parquet")
    
    # Read data from Doctors table
    doctors_df = spark.read.jdbc(url=db_url, table="Doctors", properties=db_properties)
    if check_df_not_empty(doctors_df, "Doctors"):
        write_df_to_s3(doctors_df, f"s3a://datawarehouse360/healthcare/doctors", "parquet")

    # Read data from Appointments table
    appointments_df = spark.read.jdbc(url=db_url, table="Appointments", properties=db_properties)
    if check_df_not_empty(appointments_df, "Appointments"):
        write_df_to_s3(appointments_df, f"s3a://datawarehouse360/healthcare/appointments", "parquet")

    # Read data from Treatments table
    treatments_df = spark.read.jdbc(url=db_url, table="Treatments", properties=db_properties)
    if check_df_not_empty(treatments_df, "Treatments"):
        write_df_to_s3(treatments_df, f"s3a://datawarehouse360/healthcare/treatments", "parquet")

    # Read data from Medications table
    medications_df = spark.read.jdbc(url=db_url, table="Medications", properties=db_properties)
    if check_df_not_empty(medications_df, "Medications"):
        write_df_to_s3(medications_df, f"s3a://datawarehouse360/healthcare/medications", "parquet")

    # Read data from Prescriptions table
    prescriptions_df = spark.read.jdbc(url=db_url, table="Prescriptions", properties=db_properties)
    if check_df_not_empty(prescriptions_df, "Prescriptions"):
        write_df_to_s3(prescriptions_df, f"s3a://datawarehouse360/healthcare/prescriptions", "parquet")

    # Read data from Billing table
    billing_df = spark.read.jdbc(url=db_url, table="Billing", properties=db_properties)
    if check_df_not_empty(billing_df, "Billing"):
        write_df_to_s3(billing_df, f"s3a://datawarehouse360/healthcare/billing", "parquet")

    # Read data from MedicalHistory table
    medical_history_df = spark.read.jdbc(url=db_url, table="MedicalHistory", properties=db_properties)
    if check_df_not_empty(medical_history_df, "MedicalHistory"):
        write_df_to_s3(medical_history_df, f"s3a://datawarehouse360/healthcare/medical_history", "parquet")

    print("Data successfully written to S3.")

except Exception as e:
    print(f"Error processing data: {e}")
