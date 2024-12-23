# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

def createMillRefRegexPatternList():
    patterns = []
    # Each item is a 3-element tuple (preprocess, regex_pattern, extraction)
    patterns.append((lambda col:F.left(col, F.lit(16)), r'\d{16}', lambda col:F.left(col, F.lit(16))))
    patterns.append((lambda col:F.left(col, F.lit(7)), r'\d{7}', lambda col:F.left(col, F.lit(7))))
    patterns.append((lambda col:F.left(col, F.lit(6)), r'\d{6}', lambda col:F.left(col, F.lit(6))))
    patterns.append((lambda col:F.left(col, F.lit(6)), r'RNH098', lambda col:F.lit('RNH098')))
    patterns.append((lambda col:F.substring(col, 15, 1), r'\D', lambda col:F.left(col, F.lit(14))))
    patterns.append((lambda col:col, r'.+', lambda col:F.left(col, F.lit(16))))
    return patterns


# COMMAND ----------




def pacs_IdentifyMillRefPattern(pattern_list, column):    
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(nested)

    return nested
    


# COMMAND ----------


def pacs_MillRefToAccessionNbr(pattern_list, column):
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(nested)

    return nested
    
    
