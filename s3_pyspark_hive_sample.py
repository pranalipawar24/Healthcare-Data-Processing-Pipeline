from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkSQLTransformationAndWriteToS3") \
    .enableHiveSupport() \
    .getOrCreate()

# Define S3 bucket
s3_bucket = "mydatahealth"

# Load data from Hive
spark.sql("USE healthcare")

# Transformation: Calculate Average Age of Patients
spark.sql("""
    CREATE OR REPLACE VIEW average_patient_age AS
    SELECT ROUND(AVG(DATEDIFF(CURRENT_DATE, TO_DATE(DateOfBirthFormatted, 'dd-MM-yyyy'))/365.25), 2) AS AverageAge
    FROM patients_with_formatted_dob
""")

# Transformation: Calculate Total Revenue by Doctor
spark.sql("""
    CREATE OR REPLACE VIEW total_revenue_by_doctor AS
    SELECT d.DoctorID,
           d.FullName,
           SUM(b.Amount) AS TotalRevenue
    FROM Billing b
    JOIN doctors_with_full_name d ON b.DoctorID = d.DoctorID
    GROUP BY d.DoctorID, d.FullName
""")

# Transformation: Calculate Average Number of Treatments per Appointment
spark.sql("""
    CREATE OR REPLACE VIEW average_treatments_per_appointment AS
    SELECT AppointmentID,
           ROUND(AVG(t.TotalTreatments), 2) AS AvgTreatmentsPerAppointment
    FROM treatments_count t
    GROUP BY AppointmentID
""")

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
    SELECT d.DoctorID,
           d.FullName,
           COUNT(DISTINCT p.MedicationName) AS UniqueMedications
    FROM prescriptions_with_medication p
    JOIN doctors_with_full_name d ON p.DoctorID = d.DoctorID
    GROUP BY d.DoctorID, d.FullName
""")

# Transformation: Calculate Appointment Duration
spark.sql("""
    CREATE OR REPLACE VIEW appointment_duration AS
    SELECT AppointmentID,
           PatientID,
           DoctorID,
           AppointmentDate,
           EndDate,
           ROUND((UNIX_TIMESTAMP(EndDate) - UNIX_TIMESTAMP(AppointmentDate))/60, 2) AS DurationMinutes
    FROM Appointments
""")

# Transformation: Calculate Total and Average Billing Amount per Appointment Period
spark.sql("""
    CREATE OR REPLACE VIEW billing_per_appointment_period AS
    SELECT AppointmentPeriod,
           SUM(Amount) AS TotalBilling,
           ROUND(AVG(Amount), 2) AS AvgBilling
    FROM appointments_with_period a
    JOIN Billing b ON a.AppointmentID = b.AppointmentID
    GROUP BY AppointmentPeriod
""")

# Transformation: Identify Patients with Multiple Conditions
spark.sql("""
    CREATE OR REPLACE VIEW patients_with_multiple_conditions AS
    SELECT PatientID,
           COUNT(Condition) AS ConditionCount
    FROM MedicalHistory
    GROUP BY PatientID
    HAVING ConditionCount > 1
""")

# Transformation: Calculate Average Appointment Duration by Doctor
spark.sql("""
    CREATE OR REPLACE VIEW average_appointment_duration_by_doctor AS
    SELECT d.DoctorID,
           d.FullName,
           ROUND(AVG(a.DurationMinutes), 2) AS AvgDuration
    FROM appointment_duration a
    JOIN doctors_with_full_name d ON a.DoctorID = d.DoctorID
    GROUP BY d.DoctorID, d.FullName
""")

# Transformation: Calculate Total Treatments per Patient
spark.sql("""
    CREATE OR REPLACE VIEW total_treatments_per_patient AS
    SELECT PatientID,
           COUNT(TreatmentID) AS TotalTreatments
    FROM Treatments
    GROUP BY PatientID
""")

# Report 1: Patient Appointment Summary
patient_appointment_summary_df = spark.sql("""
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
doctor_performance_report_df = spark.sql("""
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
billing_analysis_report_df = spark.sql("""
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

# Report 9: Patients with Multiple Conditions Report
patients_with_multiple_conditions_df = spark.sql("""
    SELECT *
    FROM patients_with_multiple_conditions
""")

# Report 10: Average Appointment Duration by Doctor Report
average_appointment_duration_by_doctor_df = spark.sql("""
    SELECT *
    FROM average_appointment_duration_by_doctor
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
write_df_to_s3_csv(patients_with_multiple_conditions_df, f"s3a://{s3_bucket}/healthcare_reports/patients_with_multiple_conditions")
write_df_to_s3_csv(average_appointment_duration_by_doctor_df, f"s3a://{s3_bucket}/healthcare_reports/average_appointment_duration_by_doctor")
