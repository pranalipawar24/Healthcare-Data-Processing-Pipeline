from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkSQLTransformationAndWriteToS3") \
    .enableHiveSupport() \
    .getOrCreate()

# Define S3 bucket
s3_bucket = "mydatahealth"

# Create Hive database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare")

# Load data from Hive
spark.sql("USE healthcare")

# Transformations and report generation

# Transformation 1: Create Full Name Column in Patients and Doctors Tables
spark.sql("""
    CREATE OR REPLACE VIEW patients_with_full_name AS
    SELECT *,
           CONCAT(FirstName, ' ', LastName) AS FullName
    FROM Patients
""")

spark.sql("""
    CREATE OR REPLACE VIEW doctors_with_full_name AS
    SELECT *,
           CONCAT(FirstName, ' ', LastName) AS FullName
    FROM Doctors
""")

# Transformation 2: Format Date Columns
spark.sql("""
    CREATE OR REPLACE VIEW patients_with_formatted_dob AS
    SELECT *,
           DATE_FORMAT(DateOfBirth, 'dd-MM-yyyy') AS DateOfBirthFormatted
    FROM patients_with_full_name
""")

spark.sql("""
    CREATE OR REPLACE VIEW appointments_with_formatted_date AS
    SELECT *,
           DATE_FORMAT(AppointmentDate, 'dd-MM-yyyy HH:mm') AS AppointmentDateFormatted
    FROM Appointments
""")

# Transformation 3: Create a Summary of Appointments with Patient and Doctor Names
spark.sql("""
    CREATE OR REPLACE VIEW appointments_with_names AS
    SELECT a.AppointmentID,
           p.FullName AS PatientName,
           d.FullName AS DoctorName,
           a.AppointmentDateFormatted,
           a.Reason
    FROM appointments_with_formatted_date a
    JOIN patients_with_formatted_dob p ON a.PatientID = p.PatientID
    JOIN doctors_with_full_name d ON a.DoctorID = d.DoctorID
""")

# Transformation 4: Calculate Total Amount Billed for Each Patient
spark.sql("""
    CREATE OR REPLACE VIEW total_billed AS
    SELECT PatientID,
           SUM(Amount) AS TotalBilled
    FROM Billing
    GROUP BY PatientID
""")

# Transformation 5: Count the Number of Appointments per Patient
spark.sql("""
    CREATE OR REPLACE VIEW appointments_count AS
    SELECT PatientID,
           COUNT(AppointmentID) AS AppointmentCount
    FROM Appointments
    GROUP BY PatientID
""")

# Transformation 6: Filter Patients with Specific Conditions
spark.sql("""
    CREATE OR REPLACE VIEW hypertension_patients AS
    SELECT PatientID,
           Condition
    FROM MedicalHistory
    WHERE Condition = 'Hypertension'
""")

# Transformation 7: Calculate Average Billing Amount per Appointment
spark.sql("""
    CREATE OR REPLACE VIEW average_billing AS
    SELECT AppointmentID,
           ROUND(AVG(Amount), 2) AS AverageBillingAmount
    FROM Billing
    GROUP BY AppointmentID
""")

# Transformation 8: Find Maximum and Minimum Billing Amount for Each Patient
spark.sql("""
    CREATE OR REPLACE VIEW max_min_billing AS
    SELECT PatientID,
           MAX(Amount) AS MaxBillingAmount,
           MIN(Amount) AS MinBillingAmount
    FROM Billing
    GROUP BY PatientID
""")

# Transformation 9: Create a Column Indicating Appointment Period (Morning/Afternoon)
spark.sql("""
    CREATE OR REPLACE VIEW appointments_with_period AS
    SELECT *,
           CASE
               WHEN HOUR(AppointmentDate) < 12 THEN 'Morning'
               ELSE 'Afternoon'
           END AS AppointmentPeriod
    FROM appointments_with_formatted_date
""")

# Transformation 10: Join Prescriptions with Medications to Get Medication Names
spark.sql("""
    CREATE OR REPLACE VIEW prescriptions_with_medication AS
    SELECT p.PrescriptionID,
           p.PatientID,
           p.DoctorID,
           m.MedicationName,
           p.DosageInstructions,
           p.PrescriptionDate
    FROM Prescriptions p
    JOIN Medications m ON p.MedicationID = m.MedicationID
""")

# Transformation 11: Aggregate Medical History by Condition and Count Patients
spark.sql("""
    CREATE OR REPLACE VIEW condition_count AS
    SELECT Condition,
           COUNT(PatientID) AS PatientCount
    FROM MedicalHistory
    GROUP BY Condition
    ORDER BY PatientCount DESC
""")

# Transformation 12: Join Billing Data with Appointments to Get Complete Billing Information
spark.sql("""
    CREATE OR REPLACE VIEW billing_with_appointments AS
    SELECT b.BillingID,
           b.PatientID,
           b.AppointmentID,
           b.Amount,
           b.BillingDate,
           a.Reason,
           a.AppointmentPeriod
    FROM Billing b
    JOIN appointments_with_period a ON b.AppointmentID = a.AppointmentID
""")

# Transformation 13: Calculate the Total Number of Treatments per Patient
spark.sql("""
    CREATE OR REPLACE VIEW treatments_count AS
    SELECT a.PatientID,
           COUNT(t.TreatmentID) AS TotalTreatments
    FROM Treatments t
    JOIN appointments_with_period a ON t.AppointmentID = a.AppointmentID
    GROUP BY a.PatientID
""")

# Transformation 14: Calculate Average and Total Medications Prescribed per Doctor
spark.sql("""
    CREATE OR REPLACE VIEW doctor_medication_stats AS
    SELECT d.DoctorID,
           d.FullName,
           COUNT(p.MedicationID) AS TotalMedications,
           ROUND(AVG(p.MedicationID), 2) AS AverageMedications
    FROM prescriptions_with_medication p
    JOIN doctors_with_full_name d ON p.DoctorID = d.DoctorID
    GROUP BY d.DoctorID, d.FullName
""")

# Report 1: Patient Appointment Summary
spark.sql("""
    CREATE OR REPLACE VIEW patient_appointment_summary AS
    SELECT a.PatientID,
           a.PatientName,
           a.AppointmentDateFormatted,
           a.Reason,
           t.TotalBilled,
           c.AppointmentCount,
           COALESCE(h.Condition, 'No Hypertension') AS HypertensionStatus
    FROM appointments_with_names a
    JOIN total_billed t ON a.PatientID = t.PatientID
    JOIN appointments_count c ON a.PatientID = c.PatientID
    LEFT JOIN hypertension_patients h ON a.PatientID = h.PatientID
""")

# Report 2: Doctor Performance Report
spark.sql("""
    CREATE OR REPLACE VIEW doctor_performance_report AS
    SELECT d.DoctorID,
           d.FullName,
           COUNT(p.PrescriptionID) AS TotalPrescriptions,
           AVG(dm.AverageMedications) AS AvgMedicationsPrescribed
    FROM prescriptions_with_medication p
    JOIN doctor_medication_stats dm ON p.DoctorID = dm.DoctorID
    JOIN doctors_with_full_name d ON p.DoctorID = d.DoctorID
    GROUP BY d.DoctorID, d.FullName
""")

# Report 3: Billing Analysis Report
spark.sql("""
    CREATE OR REPLACE VIEW billing_analysis_report AS
    SELECT b.PatientID,
           b.AppointmentID,
           b.Amount,
           b.BillingDate,
           b.Reason,
           b.AppointmentPeriod,
           a.AverageBillingAmount,
           m.MaxBillingAmount,
           m.MinBillingAmount
    FROM billing_with_appointments b
    JOIN average_billing a ON b.AppointmentID = a.AppointmentID
    JOIN max_min_billing m ON b.PatientID = m.PatientID
""")

# Report 4: Condition Distribution Report
spark.sql("""
    CREATE OR REPLACE VIEW condition_distribution_report AS
    SELECT Condition,
           PatientCount,
           (PatientCount * 100.0 / SUM(PatientCount) OVER ()) AS Percentage
    FROM condition_count
""")

# Report 5: Treatment Summary Report
spark.sql("""
    CREATE OR REPLACE VIEW treatment_summary_report AS
    SELECT t.PatientID,
           t.TotalTreatments,
           a.AppointmentPeriod
    FROM treatments_count t
    JOIN appointments_with_period a ON t.PatientID = a.PatientID
""")

# Show final reports
spark.sql("SELECT * FROM patient_appointment_summary").show(truncate=False)
spark.sql("SELECT * FROM doctor_performance_report").show(truncate=False)
spark.sql("SELECT * FROM billing_analysis_report").show(truncate=False)
spark.sql("SELECT * FROM condition_distribution_report").show(truncate=False)
spark.sql("SELECT * FROM treatment_summary_report").show(truncate=False)

# Write final reports to S3
spark.sql("CREATE TABLE IF NOT EXISTS patient_appointment_summary AS SELECT * FROM patient_appointment_summary")
spark.sql("CREATE TABLE IF NOT EXISTS doctor_performance_report AS SELECT * FROM doctor_performance_report")
spark.sql("CREATE TABLE IF NOT EXISTS billing_analysis_report AS SELECT * FROM billing_analysis_report")
spark.sql("CREATE TABLE IF NOT EXISTS condition_distribution_report AS SELECT * FROM condition_distribution_report")
spark.sql("CREATE TABLE IF NOT EXISTS treatment_summary_report AS SELECT * FROM treatment_summary_report")

# Show final reports
patient_appointment_summary_df = spark.sql("SELECT * FROM patient_appointment_summary")
doctor_performance_report_df = spark.sql("SELECT * FROM doctor_performance_report")
billing_analysis_report_df = spark.sql("SELECT * FROM billing_analysis_report")
condition_distribution_report_df = spark.sql("SELECT * FROM condition_distribution_report")
treatment_summary_report_df = spark.sql("SELECT * FROM treatment_summary_report")

# Function to write DataFrame to S3 if not empty
def write_df_to_s3(df, path):
    if df.count() > 0:
        df.write.mode("overwrite").csv(path)
        print(f"DataFrame written to {path}")
    else:
        print(f"DataFrame is empty. Not writing to {path}")

# Write final reports to S3
write_df_to_s3(patient_appointment_summary_df, f"s3a://{s3_bucket}/healthcare_reports/patient_appointment_summary")
write_df_to_s3(doctor_performance_report_df, f"s3a://{s3_bucket}/healthcare_reports/doctor_performance_report")
write_df_to_s3(billing_analysis_report_df, f"s3a://{s3_bucket}/healthcare_reports/billing_analysis_report")
write_df_to_s3(condition_distribution_report_df, f"s3a://{s3_bucket}/healthcare_reports/condition_distribution_report")
write_df_to_s3(treatment_summary_report_df, f"s3a://{s3_bucket}/healthcare_reports/treatment_summary_report")
"""
Using the s3a connector for writing Spark DataFrames to S3 is
 a best practice that enhances performance, reliability, and compatibility with AWS 
 services, making it a preferred choice for big data applications.
 """
 
print(f"spark_sql_transformation_and_write_to_s3 final report done")
 
 # Function to write DataFrame to S3 if not empty (parquet)
#def write_df_to_s3(df, path):
    #if df.count() > 0:
      #  df.write.mode("overwrite").parquet(path)
       # print(f"DataFrame written to {path}")
    #else:
        #print(f"DataFrame is empty. Not writing to {path}")