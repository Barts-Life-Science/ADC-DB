# Databricks notebook source
@dlt.table(name="rde_pharmacyorders_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pharmacyorders_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pharmacyorders")

    orders = spark.table("4_prod.raw.mill_dir_orders").alias("O")
    encounter = dlt.read("rde_encounter").alias("ENC")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")

    return (
        orders.filter(col("ADC_UPDT") > max_adc_updt)
        .join(encounter, col("O.ENCNTR_ID") == col("ENC.ENCNTR_ID"), "inner")
        .join(code_value_ref.alias("Activity"), col("O.ACTIVITY_TYPE_CD") == col("Activity.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Cancel"), col("O.CANCELED_REASON_CD") == col("Cancel.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ClinicCat"), col("O.CLINICAL_CATEGORY_CD") == col("ClinicCat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("OrderTyp"), col("O.ORDERABLE_TYPE_CD") == col("OrderTyp.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("LastOStat"), col("O.ORDER_STATUS_CD") == col("LastOStat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Prio"), col("O.PRIORITY_CD") == col("Prio.CODE_VALUE_CD"), "left")
        .select(
            col("O.ORDER_ID").cast(StringType()).alias("OrderID"),
            col("ENC.MRN").cast(StringType()).alias("MRN"),
            col("ENC.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("O.ENCNTR_ID").cast(StringType()).alias("ENCNTRID"),
            col("ENC.ENC_TYPE").cast(StringType()).alias("EncType"),
            col("O.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("O.ORDER_DT_TM").cast(StringType()).alias("OrderDate"),
            col("O.LAST_ORDER_STATUS_DT_TM").cast(StringType()).alias("LastOrderStatusDateTime"),
            col("O.REQUESTED_START_DT_TM").cast(StringType()).alias("ReqStartDateTime"),
            col("O.ORDER_MNEMONIC").cast(StringType()).alias("OrderText"),
            col("O.ORDER_COMMENT").cast(StringType()).alias("Comments"),
            col("T.ORDER_DETAIL_DISPLAY_LINE").cast(StringType()).alias("OrderDetails"),
            col("LastOStat.CODE_DESC_TXT").cast(StringType()).alias("LastOrderStatus"),
            col("ClinicCat.CODE_DESC_TXT").cast(StringType()).alias("ClinicalCategory"),
            col("Activity.CODE_DESC_TXT").cast(StringType()).alias("ActivityDesc"),
            col("OrderTyp.CODE_DESC_TXT").cast(StringType()).alias("OrderableType"),
            col("Prio.CODE_DESC_TXT").cast(StringType()).alias("PriorityDesc"),
            col("Cancel.CODE_DESC_TXT").cast(StringType()).alias("CancelledReason"),
            col("O.CANCELED_DT_TM").cast(StringType()).alias("CancelledDT"),
            col("O.COMPLETED_DT_TM").cast(StringType()).alias("CompletedDT"),
            col("O.DISCONTINUE_DT_TM").cast(StringType()).alias("DiscontinuedDT"),
            col("O.CONCEPT_CKI_IDENT").cast(StringType()).alias("ConceptIdent"),
            greatest(col("O.ADC_UPDT"), col("ENC.ADC_UPDT"), col("T.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("O.ORDERABLE_TYPE_CD") == 2516)
        .filter(col("O.ORDER_STATUS_CD").isin(2543, 2545, 2547, 2548, 2550, 2552, 643466))
        .filter(col("O.CLINICAL_CATEGORY_CD") == 10577)
        .filter(col("O.ACTIVITY_TYPE_CD") == 705)
        .filter(col("O.ACTIVE_IND") == 1)
    )

@dlt.view(name="pharmacyorders_update")
def pharmacyorders_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pharmacyorders_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pharmacyorders",
    comment="Incrementally updated pharmacy orders data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,OrderID"
    }
)

dlt.apply_changes(
    target = "rde_pharmacyorders",
    source = "pharmacyorders_update",
    keys = ["PERSONID", "OrderID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_powerforms_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def powerforms_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_powerforms")

    doc_response = spark.table("4_prod.raw.pi_cde_doc_response").alias("DOC")
    encounter = dlt.read("rde_encounter").alias("Enc")
    doc_ref = spark.table("4_prod.raw.pi_lkp_cde_doc_ref").alias("Dref")
    code_value_ref = spark.table("4_prod.raw.pi_lkp_cde_code_value_ref").alias("Cref")

    # Filter each table separately
    filtered_doc_response = doc_response.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_doc_ref = doc_ref.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_code_value_ref = code_value_ref.filter(col("ADC_UPDT") > max_adc_updt)

    # Get relevant encounter IDs
    relevant_encounter_ids = filtered_doc_response.select("ENCNTR_ID").distinct()

    return (
        doc_response
        .join(relevant_encounter_ids, "ENCNTR_ID", "inner")
        .join(encounter, "ENCNTR_ID", "inner")
        .join(doc_ref, col("DOC.DOC_INPUT_ID") == col("Dref.DOC_INPUT_KEY"), "left")
        .join(code_value_ref, col("DOC.FORM_STATUS_CD") == col("Cref.CODE_VALUE_CD"), "left")
        .select(
            col("Enc.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("Enc.MRN").cast(StringType()).alias("MRN"),
            col("Enc.ENCNTR_ID").cast(StringType()).alias("ENCNTR_ID"),
            col("DOC.PERFORMED_DT_TM").cast(StringType()).alias("PerformDate"),
            col("DOC.DOC_RESPONSE_KEY").cast(StringType()).alias("DOC_RESPONSE_KEY"),
            col("Dref.FORM_DESC_TXT").cast(StringType()).alias("Form"),
            col("DOC.FORM_EVENT_ID").cast(StringType()).alias("FormID"),
            col("Dref.SECTION_DESC_TXT").cast(StringType()).alias("Section"),
            col("DOC.SECTION_EVENT_ID").cast(StringType()).alias("SectionID"),
            col("Dref.ELEMENT_LABEL_TXT").cast(StringType()).alias("Element"),
            col("DOC.ELEMENT_EVENT_ID").cast(StringType()).alias("ElementID"),
            col("Dref.GRID_NAME_TXT").cast(StringType()).alias("Component"),
            col("Dref.GRID_COLUMN_DESC_TXT").cast(StringType()).alias("ComponentDesc"),
            col("DOC.GRID_EVENT_ID").cast(StringType()).alias("ComponentID"),
            col("DOC.RESPONSE_VALUE_TXT").cast(StringType()).alias("Response"),
            when(col("DOC.RESPONSE_VALUE_TXT").cast("double").isNotNull(), lit(1)).otherwise(lit(0)).alias("ResponseNumeric"),
            col("Cref.CODE_DESC_TXT").cast(StringType()).alias("Status"),
            greatest(col("DOC.ADC_UPDT"), col("Enc.ADC_UPDT"), col("Dref.ADC_UPDT"), col("Cref.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="powerforms_update")
def powerforms_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_powerforms_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_powerforms",
    comment="Incrementally updated Powerforms data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID,DOC_RESPONSE_KEY"
    }
)

dlt.apply_changes(
    target = "rde_powerforms",
    source = "powerforms_update",
    keys = ["ENCNTR_ID", "DOC_RESPONSE_KEY"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_mat_nnu_exam_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def mat_nnu_exam_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mat_nnu_exam")

    nnu_routine_examination = spark.table("4_prod.raw.badgernet_nnu_routine_examination").alias("NNUExam")
    national_id_ep_idx = spark.table("4_prod.raw.badgernet_national_id_ep_idx").alias("NIE")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")

    return (
        nnu_routine_examination
        .join(national_id_ep_idx, col("NIE.EntityID") == col("NNUExam.entityid"), "left")
        .join(patient_demographics, col("NIE.nationalid") == col("PDEM.NHS_Number"), "inner")
        .select(
            col("PDEM.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PDEM.MRN").cast(StringType()).alias("MRN"),
            col("NIE.nationalid").cast(StringType()).alias("NHS_Number"),
            coalesce(col("NNUExam.DateOfExamination"), col("NNUExam.RecordTimestamp")).cast(StringType()).alias("ExamDate"),
            col("NNUExam.HeadCircumference").cast(FloatType()).alias("HeadCircumference"),
            col("NNUExam.Skin").cast(StringType()).alias("Skin"),
            col("NNUExam.SkinComments").cast(StringType()).alias("SkinComments"),
            col("NNUExam.Cranium").cast(StringType()).alias("Cranium"),
            col("NNUExam.CraniumComments").cast(StringType()).alias("CraniumComments"),
            col("NNUExam.Fontanelle").cast(StringType()).alias("Fontanelle"),
            col("NNUExam.FontanelleComments").cast(StringType()).alias("FontanelleComments"),
            col("NNUExam.Sutures").cast(StringType()).alias("Sutures"),
            col("NNUExam.SuturesComments").cast(StringType()).alias("SuturesComments"),
            col("NNUExam.RedReflex").cast(StringType()).alias("RedReflex"),
            col("NNUExam.RedReflexComments").cast(StringType()).alias("RedReflexComments"),
            col("NNUExam.RedReflexRight").cast(StringType()).alias("RedReflexRight"),
            col("NNUExam.RedReflexCommentsRight").cast(StringType()).alias("RedReflexCommentsRight"),
            col("NNUExam.Ears").cast(StringType()).alias("Ears"),
            col("NNUExam.EarsComments").cast(StringType()).alias("EarsComments"),
            col("NNUExam.PalateSuck").cast(StringType()).alias("PalateSuck"),
            col("NNUExam.PalateSuckComments").cast(StringType()).alias("PalateSuckComments"),
            col("NNUExam.Spine").cast(StringType()).alias("Spine"),
            col("NNUExam.SpineComments").cast(StringType()).alias("SpineComments"),
            col("NNUExam.Breath").cast(StringType()).alias("Breath"),
            col("NNUExam.BreathComments").cast(StringType()).alias("BreathComments"),
            col("NNUExam.Heart").cast(StringType()).alias("Heart"),
            col("NNUExam.HeartComments").cast(StringType()).alias("HeartComments"),
            col("NNUExam.Femoral").cast(StringType()).alias("Femoral"),
            col("NNUExam.FemoralComments").cast(StringType()).alias("FemoralComments"),
            col("NNUExam.FemoralRight").cast(StringType()).alias("FemoralRight"),
            col("NNUExam.FemoralCommentsRight").cast(StringType()).alias("FemoralCommentsRight"),
            col("NNUExam.Abdomen").cast(StringType()).alias("Abdomen"),
            col("NNUExam.AbdomenComments").cast(StringType()).alias("AbdomenComments"),
            col("NNUExam.Genitalia").cast(StringType()).alias("Genitalia"),
            col("NNUExam.GenitaliaComments").cast(StringType()).alias("GenitaliaComments"),
            col("NNUExam.Testicles").cast(StringType()).alias("Testicles"),
            col("NNUExam.TesticlesComments").cast(StringType()).alias("TesticlesComments"),
            col("NNUExam.Anus").cast(StringType()).alias("Anus"),
            col("NNUExam.AnusComments").cast(StringType()).alias("AnusComments"),
            col("NNUExam.Hands").cast(StringType()).alias("Hands"),
            col("NNUExam.HandsComments").cast(StringType()).alias("HandsComments"),
            col("NNUExam.Feet").cast(StringType()).alias("Feet"),
            col("NNUExam.FeetComments").cast(StringType()).alias("FeetComments"),
            col("NNUExam.Hips").cast(StringType()).alias("Hips"),
            col("NNUExam.HipsComments").cast(StringType()).alias("HipsComments"),
            col("NNUExam.HipsRight").cast(StringType()).alias("HipsRight"),
            col("NNUExam.HipRightComments").cast(StringType()).alias("HipRightComments"),
            col("NNUExam.Tone").cast(StringType()).alias("Tone"),
            col("NNUExam.ToneComments").cast(StringType()).alias("ToneComments"),
            col("NNUExam.Movement").cast(StringType()).alias("Movement"),
            col("NNUExam.MovementComments").cast(StringType()).alias("MovementComments"),
            col("NNUExam.Moro").cast(StringType()).alias("Moro"),
            col("NNUExam.MoroComments").cast(StringType()).alias("MoroComments"),
            col("NNUExam.Overall").cast(StringType()).alias("Overall"),
            col("NNUExam.OverallComments").cast(StringType()).alias("OverallComments"),
            col("NNUExam.NameOfExaminer").cast(StringType()).alias("NameOfExaminer"),
            col("NNUExam.Palate").cast(StringType()).alias("Palate"),
            col("NNUExam.PalateComments").cast(StringType()).alias("PalateComments"),
            col("NNUExam.SuckingReflex").cast(StringType()).alias("SuckingReflex"),
            col("NNUExam.SuckingReflexComments").cast(StringType()).alias("SuckingReflexComments"),
            col("NNUExam.EarsLeft").cast(StringType()).alias("EarsLeft"),
            col("NNUExam.EarsCommentsLeft").cast(StringType()).alias("EarsCommentsLeft"),
            col("NNUExam.EarsRight").cast(StringType()).alias("EarsRight"),
            col("NNUExam.EarsCommentsRight").cast(StringType()).alias("EarsCommentsRight"),
            col("NNUExam.Eyes").cast(StringType()).alias("Eyes"),
            col("NNUExam.EyesComments").cast(StringType()).alias("EyesComments"),
            col("NNUExam.Chest_NZ").cast(StringType()).alias("Chest_NZ"),
            col("NNUExam.ChestComments_NZ").cast(StringType()).alias("ChestComments_NZ"),
            col("NNUExam.Mouth_NZ").cast(StringType()).alias("Mouth_NZ"),
            col("NNUExam.MouthComments_NZ").cast(StringType()).alias("MouthComments_NZ"),
            col("NNUExam.Growth_NZ").cast(StringType()).alias("Growth_NZ"),
            col("NNUExam.GrowthComments_NZ").cast(StringType()).alias("GrowthComments_NZ"),
            col("NNUExam.Grasp").cast(StringType()).alias("Grasp"),
            col("NNUExam.GraspComments").cast(StringType()).alias("GraspComments"),
            col("NNUExam.Femorals_NZ").cast(StringType()).alias("Femorals_NZ"),
            col("NNUExam.FemoralsComments_NZ").cast(StringType()).alias("FemoralsComments_NZ"),
            col("NNUExam.InguinalHernia").cast(StringType()).alias("InguinalHernia"),
            col("NNUExam.InguinalHerniaComments").cast(StringType()).alias("InguinalHerniaComments"),
            col("NNUExam.GeneralComments").cast(StringType()).alias("GeneralComments"),
            col("NNUExam.SyncScope").cast(StringType()).alias("SyncScope"),
            greatest(col("NNUExam.ADC_UPDT"), col("NIE.ADC_UPDT"), col("PDEM.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("PERSON_ID").isNotNull())
    )

@dlt.view(name="mat_nnu_exam_update")
def mat_nnu_exam_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_mat_nnu_exam_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_mat_nnu_exam",
    comment="Incrementally updated MAT NNU Exam data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,ExamDate"
    }
)

dlt.apply_changes(
    target = "rde_mat_nnu_exam",
    source = "mat_nnu_exam_update",
    keys = ["PERSON_ID", "ExamDate"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
