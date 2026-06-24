from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkSQLTransformationAndWriteToS3") \
    .enableHiveSupport() \
    .getOrCreate()

# Define S3 bucket
s3_bucket = "datawarehouse360"

# Load data from Hive
spark.sql("USE healthcare")

# Transformation: Identify High-Billing Patients
spark.sql("""
    CREATE OR REPLACE VIEW high_billing_patients AS
    SELECT PatientID,
           TotalBilled
    FROM total_billed
    WHERE TotalBilled > 1000
""")

# Transformation: Count Number of Unique Medications Prescribed per Doctor
spark.sql("""
    CREATE OR REPLACE VIEW unique_medications_per_doctor AS
    SELECT p.DoctorID,
           COUNT(DISTINCT p.MedicationName) AS UniqueMedications
    FROM prescriptions_with_medication p
    GROUP BY p.DoctorID
""")

# Transformation: Calculate Total and Average Billing Amount per Appointment Period
spark.sql("""
    CREATE OR REPLACE VIEW billing_per_appointment_period AS
    SELECT a.AppointmentPeriod,
           SUM(b.Amount) AS TotalBilling,
           ROUND(AVG(b.Amount), 2) AS AvgBilling
    FROM appointments_with_period a
    JOIN billing_with_appointments b ON a.AppointmentID = b.AppointmentID
    GROUP BY a.AppointmentPeriod
""")

# Report 1: Patient Appointment Summary
patient_appointment_summary_df = spark.sql("""
    SELECT a.AppointmentID,
           a.PatientName,
           a.AppointmentDateFormatted,
           a.Reason,
           t.TotalBilled,
           c.AppointmentCount,
           COALESCE(h.Condition, 'No Hypertension') AS HypertensionStatus
    FROM appointments_with_names a
    JOIN total_billed t ON a.AppointmentID = t.PatientID
    JOIN appointments_count c ON a.AppointmentID = c.PatientID
    LEFT JOIN hypertension_patients h ON a.AppointmentID = h.PatientID
""")

# Report 2: Doctor Performance Report
doctor_performance_report_df = spark.sql("""
    SELECT p.DoctorID,
           COUNT(p.PrescriptionID) AS TotalPrescriptions,
           AVG(dm.AverageMedications) AS AvgMedicationsPrescribed
    FROM prescriptions_with_medication p
    JOIN doctor_medication_stats dm ON p.DoctorID = dm.DoctorID
    GROUP BY p.DoctorID
""")

# Report 3: Billing Analysis Report
billing_analysis_report_df = spark.sql("""
    SELECT b.BillingPatientID,
           b.AppointmentID,
           b.Amount,
           b.BillingDate,
           b.Reason,
           a.AverageBillingAmount,
           m.MaxBillingAmount,
           m.MinBillingAmount
    FROM billing_with_appointments b
    JOIN average_billing a ON b.AppointmentID = a.AppointmentID
    JOIN max_min_billing m ON b.AppointmentID = m.PatientID
""")

# Report 4: Condition Distribution Report
condition_distribution_report_df = spark.sql("""
    SELECT Condition,
           PatientCount,
           (PatientCount * 100.0 / SUM(PatientCount) OVER ()) AS Percentage
    FROM condition_count
""")

# Report 5: Treatment Summary Report
treatment_summary_report_df = spark.sql("""
    SELECT t.PatientID,
           t.TotalTreatments,
           a.AppointmentPeriod
    FROM treatments_count t
    JOIN appointments_with_period a ON t.PatientID = a.PatientID
""")

# Report 6: High Billing Patients Report
high_billing_patients_df = spark.sql("""
    SELECT *
    FROM high_billing_patients
""")

# Report 7: Unique Medications per Doctor Report
unique_medications_per_doctor_df = spark.sql("""
    SELECT *
    FROM unique_medications_per_doctor
""")

# Report 8: Billing per Appointment Period Report
billing_per_appointment_period_df = spark.sql("""
    SELECT *
    FROM billing_per_appointment_period
""")

# Function to write DataFrame to S3 in CSV format if not empty
def write_df_to_s3_csv(df, path):
    if df.count() > 0:
        df.write.mode("overwrite").csv(path, header=True)
        print(f"DataFrame written to {path} in CSV format.")
    else:
        print(f"DataFrame is empty. Not writing to {path}")

# Write final reports to S3 in CSV format
write_df_to_s3_csv(patient_appointment_summary_df, f"s3a://{s3_bucket}/healthcare_reports/patient_appointment_summary")
write_df_to_s3_csv(doctor_performance_report_df, f"s3a://{s3_bucket}/healthcare_reports/doctor_performance_report")
write_df_to_s3_csv(billing_analysis_report_df, f"s3a://{s3_bucket}/healthcare_reports/billing_analysis_report")
write_df_to_s3_csv(condition_distribution_report_df, f"s3a://{s3_bucket}/healthcare_reports/condition_distribution_report")
write_df_to_s3_csv(treatment_summary_report_df, f"s3a://{s3_bucket}/healthcare_reports/treatment_summary_report")
write_df_to_s3_csv(high_billing_patients_df, f"s3a://{s3_bucket}/healthcare_reports/high_billing_patients")
write_df_to_s3_csv(unique_medications_per_doctor_df, f"s3a://{s3_bucket}/healthcare_reports/unique_medications_per_doctor")
write_df_to_s3_csv(billing_per_appointment_period_df, f"s3a://{s3_bucket}/healthcare_reports/billing_per_appointment_period")




print("spark_hive_write_to_s3 final report genertaion done")