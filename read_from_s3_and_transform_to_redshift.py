from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, date_format, when, sum, count, avg, max, min, round

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TransformAndWriteToRedshift") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.271") \
    .getOrCreate()

# Define S3 and Redshift connection parameters
s3_bucket = "your-bucket-name"
redshift_url = "jdbc:redshift://your-redshift-cluster-url:5439/your-database"
redshift_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Read data from S3
patients_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/patients")
doctors_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/doctors")
appointments_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/appointments")
treatments_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/treatments")
medications_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/medications")
prescriptions_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/prescriptions")
billing_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/billing")
medical_history_df = spark.read.parquet(f"s3a://{s3_bucket}/healthcare/medical_history")

# Add FullName column to Patients
patients_df = patients_df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))

# Add FullName column to Doctors
doctors_df = doctors_df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))

# Format DateOfBirth in Patients
patients_df = patients_df.withColumn("DateOfBirthFormatted", date_format(col("DateOfBirth"), "dd-MM-yyyy"))

# Format AppointmentDate in Appointments
appointments_df = appointments_df.withColumn("AppointmentDateFormatted", date_format(col("AppointmentDate"), "dd-MM-yyyy HH:mm"))

# Join Patients and Appointments
appointments_with_names_df = appointments_df.join(patients_df, "PatientID") \
    .join(doctors_df, "DoctorID") \
    .select("AppointmentID", "FullName", "AppointmentDateFormatted", "Reason")

# Rename columns for clarity
appointments_with_names_df = appointments_with_names_df.withColumnRenamed("FullName", "PatientName")

# Calculate total amount billed for each patient
total_billed_df = billing_df.groupBy("PatientID") \
    .sum("Amount") \
    .withColumnRenamed("sum(Amount)", "TotalBilled")

# Count the number of appointments per patient
appointments_count_df = appointments_df.groupBy("PatientID") \
    .count() \
    .withColumnRenamed("count", "AppointmentCount")

# Filter patients with Hypertension
hypertension_patients_df = medical_history_df.filter(col("Condition") == "Hypertension")

# Calculate the average billing amount per appointment
average_billing_df = billing_df.groupBy("AppointmentID") \
    .agg(round(avg("Amount"), 2).alias("AverageBillingAmount"))

# Find the maximum and minimum billing amount for each patient
max_min_billing_df = billing_df.groupBy("PatientID") \
    .agg(max("Amount").alias("MaxBillingAmount"), min("Amount").alias("MinBillingAmount"))

# Create a column indicating if the appointment is in the morning or afternoon
appointments_df = appointments_df.withColumn("AppointmentPeriod", 
    when(col("AppointmentDateFormatted").substr(12, 2).cast("int") < 12, "Morning").otherwise("Afternoon"))

# Join Prescriptions with Medications to get medication names
prescriptions_with_medication_df = prescriptions_df.join(medications_df, "MedicationID") \
    .select("PrescriptionID", "PatientID", "DoctorID", "MedicationName", "DosageInstructions", "PrescriptionDate")

# Aggregate medical history by condition and count patients for each condition
condition_count_df = medical_history_df.groupBy("Condition") \
    .agg(count("PatientID").alias("PatientCount")) \
    .orderBy(col("PatientCount").desc())

# Join billing data with appointments to get complete billing information
billing_with_appointments_df = billing_df.join(appointments_df, "AppointmentID") \
    .select("BillingID", "PatientID", "AppointmentID", "Amount", "BillingDate", "Reason", "AppointmentPeriod")

# Calculate the total number of treatments per patient
treatments_count_df = treatments_df.join(appointments_df, "AppointmentID") \
    .groupBy("PatientID") \
    .agg(count("TreatmentID").alias("TotalTreatments"))

# Calculate average and total medications prescribed per doctor
doctor_medication_stats_df = prescriptions_with_medication_df.groupBy("DoctorID") \
    .agg(count("MedicationID").alias("TotalMedications"), round(avg("MedicationID"), 2).alias("AverageMedications"))

# Write transformed data to Redshift
appointments_with_names_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.appointments_with_names") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

total_billed_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.total_billed") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

appointments_count_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.appointments_count") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

hypertension_patients_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.hypertension_patients") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

average_billing_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.average_billing") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

max_min_billing_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.max_min_billing") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

appointments_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.appointments_with_period") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

prescriptions_with_medication_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.prescriptions_with_medication") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

condition_count_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "healthcare.condition_count") \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("overwrite") \
    .save()

billing_with_appointments_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("db