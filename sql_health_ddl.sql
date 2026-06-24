-- Create Patients Table
CREATE TABLE Patients (
    PatientID INT PRIMARY KEY, -- Unique ID for each patient
    FirstName VARCHAR(50) NOT NULL, -- Patient's first name
    LastName VARCHAR(50) NOT NULL, -- Patient's last name
    DateOfBirth DATE NOT NULL, -- Patient's date of birth
    Gender VARCHAR(10) NOT NULL CHECK (Gender IN ('Male', 'Female', 'Other')), -- Patient's gender
    Phone VARCHAR(15), -- Patient's phone number
    Email VARCHAR(100) -- Patient's email address
);

-- Create Doctors Table
CREATE TABLE Doctors (
    DoctorID INT PRIMARY KEY, -- Unique ID for each doctor
    FirstName VARCHAR(50) NOT NULL, -- Doctor's first name
    LastName VARCHAR(50) NOT NULL, -- Doctor's last name
    Specialty VARCHAR(100) NOT NULL, -- Doctor's specialty
    Phone VARCHAR(15), -- Doctor's phone number
    Email VARCHAR(100) -- Doctor's email address
);

-- Create Appointments Table
CREATE TABLE Appointments (
    AppointmentID INT PRIMARY KEY, -- Unique ID for each appointment
    PatientID INT, -- ID of the patient for the appointment
    DoctorID INT, -- ID of the doctor for the appointment
    AppointmentDate TIMESTAMP NOT NULL, -- Date and time of the appointment
    Reason VARCHAR(255) -- Reason for the appointment
);

-- Create Treatments Table
CREATE TABLE Treatments (
    TreatmentID INT PRIMARY KEY, -- Unique ID for each treatment
    AppointmentID INT, -- ID of the appointment for the treatment
    TreatmentDescription VARCHAR(255) NOT NULL, -- Description of the treatment
    TreatmentDate DATE NOT NULL -- Date of the treatment
);

-- Create Medications Table
CREATE TABLE Medications (
    MedicationID INT PRIMARY KEY, -- Unique ID for each medication
    MedicationName VARCHAR(100) NOT NULL, -- Name of the medication
    Dosage VARCHAR(50) NOT NULL, -- Dosage of the medication
    SideEffects VARCHAR(255) -- Side effects of the medication
);

-- Create Prescriptions Table
CREATE TABLE Prescriptions (
    PrescriptionID INT PRIMARY KEY, -- Unique ID for each prescription
    PatientID INT, -- ID of the patient for the prescription
    DoctorID INT, -- ID of the doctor for the prescription
    MedicationID INT, -- ID of the medication for the prescription
    PrescriptionDate DATE NOT NULL, -- Date of the prescription
    DosageInstructions VARCHAR(255) -- Dosage instructions for the medication
);

-- Create Billing Table
CREATE TABLE Billing (
    BillingID INT PRIMARY KEY, -- Unique ID for each billing record
    PatientID INT, -- ID of the patient for the billing record
    AppointmentID INT, -- ID of the appointment for the billing record
    Amount DECIMAL(10, 2) NOT NULL, -- Billing amount
    BillingDate DATE NOT NULL -- Date of the billing
);

-- Create MedicalHistory Table
CREATE TABLE MedicalHistory (
    HistoryID INT PRIMARY KEY, -- Unique ID for each medical history record
    PatientID INT, -- ID of the patient for the medical history record
    Condition VARCHAR(255) NOT NULL, -- Medical condition
    DiagnosisDate DATE NOT NULL, -- Date of diagnosis
    Treatment VARCHAR(255) -- Treatment for the condition
);
