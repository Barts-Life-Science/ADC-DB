from pyspark.sql import SparkSession


from pyspark.sql import functions as F

import sys, os

sys.path.append("/Workspace/Shared/ADC-DB/Dev/pacs/utils/")

import pacs_data_transformations as DT

import pytest

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

def test_transformExamAccessionNumber():
    df = spark.sql("""
        SELECT *
        FROM (
        VALUES
        ('UKWXH00000123456', 'XCHES', 'VALUE_TOO_LONG', NULL, 'Mark as missing'),
        ('1234567', '1234567', 'VALUE_TOO_LONG', NULL, 'Mark as missing'),
        ('0', 'FLO123456A1234', 'FLO123456A12340', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
        ('0', '0', 'FLO123456A12340', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
        ('0', '61234567891', 'FLO123456A12340', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
        ('0', 'H123456780001C', 'H123456780001C0', 'H123456780001C', 'Remove appended ExaminationIdString'),
        ('0', '1.00*', '1.000', '1.00', 'Remove appended ExaminationIdString'),
        ('0', '1.00', '1.000', '1.00', 'Remove appended ExaminationIdString'),
        ('0', NULL, '12345670', '1234567', 'Remove appended ExaminationIdString'),
        ('0', '41234', '12345670', '1234567', 'Remove appended ExaminationIdString'),
        ('0', '1234', '1234A0', '1234A', 'Remove appended ExaminationIdString'),
        ('00', 'MA23456789', 'FLO123456A123400', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
        ('00', 'FLO123456', 'FLO1234560000000', 'FLO12345600000', 'Remove appended ExaminationIdString; ExaminationDicomStudyId stores patientid in this case'),
        ('00', '1', 'RNH0CT123456700', 'RNH0CT1234567', 'Remove appended ExaminationIdString'),
        ('00', '1', 'US-12-1234567800', 'US-12-12345678', 'Remove appended ExaminationIdString'),
        ('00', '00', 'ct12345678912300', 'ct123456789123', 'Remove appended ExaminationIdString'),
        ('0A', 'XFOOL', 'RNJ000012345670A', 'RNJ00001234567', 'Remove appended ExaminationIdString'),
        ('0A', NULL, 'RNJ000012345670A', 'RNJ00001234567', 'Remove appended ExaminationIdString'),
        ('1', '11:00', 'FLO123456A12341', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
        ('1', '3123', '12341', '1234', 'Remove appended ExaminationIdString; ExaminationDicomStudyId is not the correct accession number though it may match to some image'),
        ('01', 'RNH0XR1234567801', 'RNH0XR1234567801', 'RNH0XR12345678', 'Remove appended ExaminationIdString'),
        ('01', '1', 'RNH0XR1234567801', 'RNH0XR12345678', 'Remove appended ExaminationIdString'),
        ('01', NULL, 'RNH0XR1234567801', 'RNH0XR12345678', 'Remove appended ExaminationIdString'),
        ('2', '2', 'P1234562', 'P123456', 'Remove appended ExaminationIdString'),
        ('02', '1', 'RNJ0000012345602', 'RNJ00000123456', 'Remove appended ExaminationIdString'),
        ('10', '10', 'STG123456710', 'STG1234567', 'Remove appended ExaminationIdString'),
        ('123456', NULL, '123456123456', '123456', 'Remove appended ExaminationIdString'),
        ('123456', '41234', '123456123456', '123456', 'Remove appended ExaminationIdString'),
        ('00123456', NULL, '0012345600123456', '00123456', 'Remove appended ExaminationIdString'),
        ('123456789123456', '1', '1234567891234561', NULL, 'Mark as missing'),
        ('19123456', '3123', '191234563123', NULL, 'Mark as missing when ExaminationIdString is the correct accession number')
        ) AS tmp(ExaminationIdString, ExaminationDicomStudyId, ExaminationAccessionNumber, Expected_ExaminationAccessionNumber, ExpectedTransformationDescription)
    """)

    df = df.withColumn('ExaminationAccessionNumber_t', DT.transformExamAccessionNumber(F.col("ExaminationAccessionNumber"), F.col("ExaminationIdString")))

    diff = df.filter("Expected_ExaminationAccessionNumber != ExaminationAccessionNumber_t")

    assert 0 == diff.count(), f"{diff.count()} test samples failed at transformationExamAccessionNumber"
    
