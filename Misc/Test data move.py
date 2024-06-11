# Databricks notebook source
# Source file path
source_path = "/Volumes/2_exland/default/vone/moviesDB2.csv"
 
# Destination file path
#destination_path = "/Volumes/1_inland/dwh/vone//moviesDB4.csv"
destination_path = "/Volumes/7_dev/default/dev_test_volume/moviesDB4.csv"
 
 
# Move the file using dbutils.fs.mv
dbutils.fs.cp(source_path, destination_path)
