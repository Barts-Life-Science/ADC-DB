from pyspark.sql import SparkSession


from pyspark.sql import functions as F

import sys, os

sys.path.append("/Workspace/Shared/ADC-DB/Dev/pacs/utils/")

import pacs_data_transformations as DT

import pytest

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()


def test_millRefToAccessionNumber():
    df = spark.sql("""
        SELECT *
        FROM (
        VALUES
        ('1234567CPANSCCPANSC0', '1234567', 'Keep the first 7 char'),
        ('123456FUPLLM1', '123456', 'Keep the first 6 char'),
        ('0000002001234567XDPEA0', '0000002001234567', 'Keep the first 16 char'),
        ('RNH0XR17012345XCHESXCHES0', 'RNH0XR17012345', 'Keep the first 14 char'),
        ('RNH0MA17012345XMAMB0', 'RNH0MA17012345', 'Keep the first 14 char'),
        ('UKRLH02001234567UNECKN1', 'UKRLH02001234567', 'Keep the first 16 char'),
        ('UKRLH02001234567CCHAPCCHAP0', 'UKRLH02001234567', 'Keep the first 16 char'),
        ('UKRLH02001234567CABDOCCABDOC0', 'UKRLH02001234567', 'Keep the first 16 char'),
        ('UKRLH02001234567NC131YNC131Y0', 'UKRLH02001234567', 'Keep the first 16 char'),
        ('UKWXH02001234567NF18WONF18WO0', 'UKWXH02001234567', 'Keep the first 16 char'),
        ('UKOUT02001234567_SECTRAMSKUHMSKUH0', 'UKOUT02001234567',  'Keep the first 16 char')
        ) AS tmp(MillRefNbr, Expected_AccessionNbr, ExpectedTransformationDescription)
    """)

    patterns = DT.createMillRefRegexPatternList()
    df = df.withColumn('AccessionNbr', DT.millRefToAccessionNbr(patterns, F.col('MillRefNbr')))
    diff = df.filter("Expected_AccessionNbr != AccessionNbr")
    assert 0 == diff.count(), f"{diff.count()} test samples failed"
   

def test_millRefToExamCode():
    df = spark.sql("""
        SELECT *
        FROM (
        VALUES
        ('1234567CPANSCCPANSC0', '1234567', 'CPANSC', 'Keep the exam code without the repeat and the last digit'),
        ('123456FUPLLM1', '123456', 'FUPLLM', 'Keep the exam code without the last digit'),
        ('0000002001234567XDPEA0', '0000002001234567', 'XDPEA', 'Keep the exam code without the last digit'),
        ('RNH0XR17012345XCHESXCHES0', 'RNH0XR17012345', 'XCHES', 'Keep the exam code without the repeat and the last digit'),
        ('RNH0MA17012345XMAMB0', 'RNH0MA17012345', 'XMAMB', 'Keep the exam code without the last digit'),
        ('UKRLH02001234567UNECKN1', 'UKRLH02001234567', 'UNECKN', 'Keep the exam code without the last digit'),
        ('UKRLH02001234567CCHAPCCHAP0', 'UKRLH02001234567', 'CCHAP', 'Keep the exam code without the repeat and the last digit'),
        ('UKRLH02001234567CABDOCCABDOC0', 'UKRLH02001234567', 'CABDOC', 'Keep the exam code without the repeat and the last digit'),
        ('UKRLH02001234567NC131YNC131Y0', 'UKRLH02001234567', 'NC131Y', 'Keep the exam code without the repeat and the last digit'),
        ('UKWXH02001234567NF18WONF18WO0', 'UKWXH02001234567', 'NF18WO', 'Keep the exam code without the repeat and the last digit'),
        ('UKOUT02001234567_SECTRAMSKUHMSKUH0', 'UKOUT02001234567', 'MSKUH', 'Keep the exam code without _SECTRA, the repeat, and the last digit')

        ) AS tmp(MillRefNbr, AccessionNbr, Expected_ExamCode, ExpectedTransformationDescription)
    """)

    df = df.withColumn('ExamCode', DT.millRefToExamCode(F.col('MillRefNbr'), F.col('AccessionNbr')))
    diff = df.filter("Expected_ExamCode != ExamCode")
    assert 0 == diff.count(), f"{diff.count()} test samples failed"


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
        ('19123456', '3123', '191234563123', NULL, 'Mark as missing when ExaminationIdString is the correct accession number'),
        ('RAJ_R15123456701', '15123456701', 'VALUE_TOO_LONG', NULL, 'Mark as missing')
        ) AS tmp(ExaminationIdString, ExaminationDicomStudyId, ExaminationAccessionNumber, Expected_ExaminationAccessionNumber, ExpectedTransformationDescription)
    """)

    df = df.withColumn('ExaminationAccessionNumber_t', DT.transformExamAccessionNumber(F.col("ExaminationAccessionNumber"), F.col("ExaminationIdString")))

    diff = df.filter("Expected_ExaminationAccessionNumber != ExaminationAccessionNumber_t")

    assert 0 == diff.count(), f"{diff.count()} test samples failed"
    
