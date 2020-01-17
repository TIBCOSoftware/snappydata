-- Create schema SPD if not already present.
CREATE SCHEMA IF NOT EXISTS SPD;

-- Drop all synthetic patient related tables, if present.
DROP TABLE IF EXISTS SPD.staging_allergies ;
DROP TABLE IF EXISTS SPD.staging_careplans ;
DROP TABLE IF EXISTS SPD.staging_conditions ;
DROP TABLE IF EXISTS SPD.staging_encounters ;
DROP TABLE IF EXISTS SPD.staging_imaging_studies ;
DROP TABLE IF EXISTS SPD.staging_immunizations ;
DROP TABLE IF EXISTS SPD.staging_medications ;
DROP TABLE IF EXISTS SPD.staging_observations ;
DROP TABLE IF EXISTS SPD.staging_patients ;
DROP TABLE IF EXISTS SPD.staging_procedures ;

DROP TABLE IF EXISTS SPD.allergies ;
DROP TABLE IF EXISTS SPD.careplans ;
DROP TABLE IF EXISTS SPD.conditions ;
DROP TABLE IF EXISTS SPD.encounters ;
DROP TABLE IF EXISTS SPD.imaging_studies ;
DROP TABLE IF EXISTS SPD.immunizations ;
DROP TABLE IF EXISTS SPD.medications ;
DROP TABLE IF EXISTS SPD.observations ;
DROP TABLE IF EXISTS SPD.patients ;
DROP TABLE IF EXISTS SPD.procedures ;

-- Create required external tables.
CREATE EXTERNAL TABLE SPD.staging_patients USING csv OPTIONS (path ':dataLocation/patients.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_encounters USING csv OPTIONS (path ':dataLocation/encounters.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_allergies USING csv OPTIONS (path ':dataLocation/allergies.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_careplans USING csv OPTIONS (path ':dataLocation/careplans.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_conditions USING csv OPTIONS (path ':dataLocation/conditions.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_imaging_studies USING csv OPTIONS (path ':dataLocation/imaging_studies.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_immunizations USING csv OPTIONS (path ':dataLocation/immunizations.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_medications USING csv OPTIONS (path ':dataLocation/medications.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_observations USING csv OPTIONS (path ':dataLocation/observations.csv', header 'true', inferSchema 'true');
CREATE EXTERNAL TABLE SPD.staging_procedures USING csv OPTIONS (path ':dataLocation/procedures.csv', header 'true', inferSchema 'true');

-- Create required synthetic patient and related colocated tables.
CREATE TABLE SPD.patients using column options(PARTITION_BY 'ID', buckets '12') as (select * from SPD.staging_patients);
CREATE TABLE SPD.encounters using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_encounters);
CREATE TABLE SPD.allergies using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_allergies);
CREATE TABLE SPD.careplans using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_careplans);
CREATE TABLE SPD.conditions using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_conditions);
CREATE TABLE SPD.imaging_studies using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_imaging_studies);
CREATE TABLE SPD.immunizations using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_immunizations);
CREATE TABLE SPD.medications using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_medications);
CREATE TABLE SPD.observations using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_observations);
CREATE TABLE SPD.procedures using column options(PARTITION_BY 'PATIENT', colocate_with 'SPD.PATIENTS', buckets '12') as (select * from SPD.staging_procedures);

