from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, lit, concat, date_format, when, sum, count, avg, max, min, round
# Create a SparkSession
spark = SparkSession.builder \
    .appName("TransformAndWriteToHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Create Hive database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare")

# Load data from Hive
spark.sql("USE healthcare")

# Function to check if DataFrame is not empty
def check_df_not_empty(df, table_name):
    if df.count() > 0:
        print(f"Data found in {table_name} table.")
        return True
    else:
        print(f"No data found in {table_name} table.")
        return False

# Function to write DataFrame to Hive
def write_df_to_hive(df, table_name):
    try:
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"Data successfully written to {table_name} in Hive.")
    except Exception as e:
        print(f"Error writing data to {table_name} in Hive: {e}")

# Read data from S3 -s3a://datawarehouse360/healthcare/
patients_df = spark.read.parquet("s3a://datawarehouse360/healthcare/patients/")
doctors_df = spark.read.parquet("s3a://datawarehouse360/healthcare/doctors/")
appointments_df = spark.read.parquet("s3a://datawarehouse360/healthcare/appointments/")
treatments_df = spark.read.parquet("s3a://datawarehouse360/healthcare/treatments/")
medications_df = spark.read.parquet("s3a://datawarehouse360/healthcare/medications/")
prescriptions_df = spark.read.parquet("s3a://datawarehouse360/healthcare/prescriptions/")
billing_df = spark.read.parquet("s3a://datawarehouse360/healthcare/billing/")
medical_history_df = spark.read.parquet("s3a://datawarehouse360/healthcare/medical_history/")

if check_df_not_empty(patients_df, "Patients"):
    # Add FullName column to Patients
    patients_df = patients_df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))

if check_df_not_empty(doctors_df, "Doctors"):
    # Add FullName column to Doctors
    doctors_df = doctors_df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))

if check_df_not_empty(patients_df, "Patients"):
    # Format DateOfBirth in Patients
    patients_df = patients_df.withColumn("DateOfBirthFormatted", date_format(col("DateOfBirth"), "dd-MM-yyyy"))

if check_df_not_empty(appointments_df, "Appointments"):
    # Format AppointmentDate in Appointments
    appointments_df = appointments_df.withColumn("AppointmentDateFormatted", date_format(col("AppointmentDate"), "dd-MM-yyyy HH:mm"))

if check_df_not_empty(appointments_df, "Appointments") and check_df_not_empty(patients_df, "Patients") and check_df_not_empty(doctors_df, "Doctors"):
    # Join Patients and Appointments, rename columns to avoid ambiguity
    appointments_with_names_df = appointments_df \
        .join(patients_df.select("PatientID", col("FullName").alias("PatientFullName")), "PatientID") \
        .join(doctors_df.select("DoctorID", col("FullName").alias("DoctorFullName")), "DoctorID") \
        .select("AppointmentID", "PatientFullName", "DoctorFullName", "AppointmentDateFormatted", "Reason") \
        .withColumnRenamed("PatientFullName", "PatientName") \
        .withColumnRenamed("DoctorFullName", "DoctorName")
    write_df_to_hive(appointments_with_names_df, "healthcare.appointments_with_names")

if check_df_not_empty(billing_df, "Billing"):
    # Calculate total amount billed for each patient
    total_billed_df = billing_df.groupBy("PatientID") \
        .sum("Amount") \
        .withColumnRenamed("sum(Amount)", "TotalBilled")
    write_df_to_hive(total_billed_df, "healthcare.total_billed")

if check_df_not_empty(appointments_df, "Appointments"):
    # Count the number of appointments per patient
    appointments_count_df = appointments_df.groupBy("PatientID") \
        .count() \
        .withColumnRenamed("count", "AppointmentCount")
    write_df_to_hive(appointments_count_df, "healthcare.appointments_count")

if check_df_not_empty(medical_history_df, "MedicalHistory"):
    # Filter patients with Hypertension
    hypertension_patients_df = medical_history_df.filter(col("Condition") == "Hypertension")
    write_df_to_hive(hypertension_patients_df, "healthcare.hypertension_patients")

if check_df_not_empty(billing_df, "Billing"):
    # Calculate the average billing amount per appointment
    average_billing_df = billing_df.groupBy("AppointmentID") \
        .agg(round(("Amount"), 2).alias("AverageBillingAmount"))
    write_df_to_hive(average_billing_df, "healthcare.average_billing")

if check_df_not_empty(billing_df, "Billing"):
    # Find the maximum and minimum billing amount for each patient
    max_min_billing_df = billing_df.groupBy("PatientID") \
        .agg(max("Amount").alias("MaxBillingAmount"), min("Amount").alias("MinBillingAmount"))
    write_df_to_hive(max_min_billing_df, "healthcare.max_min_billing")

if check_df_not_empty(appointments_df, "Appointments"):
    # Create a column indicating if the appointment is in the morning or afternoon
    appointments_df = appointments_df.withColumn("AppointmentPeriod", 
        when(col("AppointmentDateFormatted").substr(12, 2).cast("int") < 12, "Morning").otherwise("Afternoon"))
    write_df_to_hive(appointments_df, "healthcare.appointments_with_period")

if check_df_not_empty(prescriptions_df, "Prescriptions") and check_df_not_empty(medications_df, "Medications"):
    # Join Prescriptions with Medications to get medication names
    prescriptions_with_medication_df = prescriptions_df.join(medications_df, "MedicationID") \
        .select("PrescriptionID", "PatientID", "DoctorID", "MedicationName", "DosageInstructions", "PrescriptionDate")
    write_df_to_hive(prescriptions_with_medication_df, "healthcare.prescriptions_with_medication")

if check_df_not_empty(medical_history_df, "MedicalHistory"):
    # Aggregate medical history by condition and count patients for each condition
    condition_count_df = medical_history_df.groupBy("Condition") \
        .agg(count("PatientID").alias("PatientCount")) \
        .orderBy(col("PatientCount").desc())
    write_df_to_hive(condition_count_df, "healthcare.condition_count")

if check_df_not_empty(billing_df, "Billing") and check_df_not_empty(appointments_df, "Appointments"):
    # Join billing data with appointments to get complete billing information
    billing_with_appointments_df = billing_df.alias("b") \
        .join(appointments_df.alias("a"), "AppointmentID") \
        .select(
            col("b.BillingID"),
            col("b.PatientID").alias("BillingPatientID"),
            col("a.PatientID").alias("AppointmentPatientID"),
            col("a.AppointmentID"),
            col("b.Amount"),
            col("b.BillingDate"),
            col("a.Reason"),
            col("a.AppointmentPeriod")
        )
    write_df_to_hive(billing_with_appointments_df, "healthcare.billing_with_appointments")

if check_df_not_empty(treatments_df, "Treatments") and check_df_not_empty(appointments_df, "Appointments"):
    # Calculate the total number of treatments per patient
    treatments_count_df = treatments_df.join(appointments_df, "AppointmentID") \
        .groupBy("PatientID") \
        .agg(count("TreatmentID").alias("TotalTreatments"))
    write_df_to_hive(treatments_count_df, "healthcare.treatments_count")

if check_df_not_empty(prescriptions_with_medication_df, "PrescriptionsWithMedication"):
    # Calculate average and total medications prescribed per doctor
    doctor_medication_stats_df = prescriptions_with_medication_df.groupBy("DoctorID") \
        .agg(count("MedicationName").alias("TotalMedications"), round(avg("MedicationName"), 2).alias("AverageMedications"))
    write_df_to_hive(doctor_medication_stats_df, "healthcare.doctor_medication_stats")

print("Data successfully transformed and written to Hive.")
