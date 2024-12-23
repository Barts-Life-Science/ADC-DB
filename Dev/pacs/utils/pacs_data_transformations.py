from pyspark.sql import functions as F


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


def identifyMillRefPattern(pattern_list, column):    
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(nested)

    return nested


def millRefToAccessionNbr(pattern_list, column):
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(nested)

    return nested


def millRefToExamCode(mill_ref_col, accession_nbr_col):
    mill_item_code = F.replace(F.left(mill_ref_col, F.length(mill_ref_col)-1), accession_nbr_col, F.lit(''))
    mill_item_code = F.replace(mill_item_code, F.lit('_SECTRA'), F.lit(''))
    mill_ref_left = F.left(mill_item_code, F.length(mill_item_code)/2)
    mill_ref_right = F.right(mill_item_code, F.length(mill_item_code)/2)
    return F.when(mill_ref_left.eqNullSafe(mill_ref_right), mill_ref_left).otherwise(mill_item_code)


def millRefToExamCode(mill_ref_col, accession_nbr_col):
    mill_item_code = F.replace(F.left(mill_ref_col, F.length(mill_ref_col)-1), accession_nbr_col, F.lit(''))
    mill_item_code = F.replace(mill_item_code, F.lit('_SECTRA'), F.lit(''))
    mill_ref_left = F.left(mill_item_code, F.length(mill_item_code)/2)
    mill_ref_right = F.right(mill_item_code, F.length(mill_item_code)/2)
    return F.when(mill_ref_left.eqNullSafe(mill_ref_right), mill_ref_left).otherwise(mill_item_code)


def transformExamAccessionNumber(exam_access_nbr_col, exam_id_str_col):
    x = F.when(exam_access_nbr_col.eqNullSafe(F.lit('VALUE_TOO_LONG')), F.lit(None)).otherwise(exam_access_nbr_col)
    x = F.when(exam_id_str_col.eqNullSafe(F.right(x, F.length(exam_id_str_col))), F.left(x, F.length(x)-F.length(exam_id_str_col))).otherwise(F.lit(None))
    return x
