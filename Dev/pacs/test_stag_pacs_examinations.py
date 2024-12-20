# Databricks notebook source
# Mock data for cleaning ExaminationAccessionNumber

df = spark.sql("""
SELECT *
FROM (
VALUES
('UKWXH00000123456', 'VALUE_TOO_LONG', NULL, 'Mark as missing'),
('1234567', 'VALUE_TOO_LONG', NULL, 'Mark as missing'),
('0', 'FLO123456A12340', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
('1', 'FLO123456A12341', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
('00', 'FLO123456A123400', 'FLO123456A1234', 'Remove appended ExaminationIdString'),
('00', 'RNH0CT123456700', 'RNH0CT1234567', 'Remove appended ExaminationIdString'),
('0', 'H123456780001C0', 'H123456780001C', 'Remove appended ExaminationIdString'),
('0', '1.000', '1.00', 'Remove appended ExaminationIdString'),
('0', '12345670', '1234567', 'Remove appended ExaminationIdString'),
('0', '1234A0', '1234A', 'Remove appended ExaminationIdString'),
('1', '12341', '1234', 'Remove appended ExaminationIdString')
) AS tmp(ExaminationIdString, ExaminationAccessionNumber, Expected_ExaminationAccessionNumber, Expected_Transformation)
""")
display(df)
