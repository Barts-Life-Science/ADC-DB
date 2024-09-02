# Databricks notebook source



@dlt.table(name="rde_powerforms_incr", table_properties={
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID",
        "skipChangeCommits": "true"
    }, temporary=True)
def powerforms_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_powerforms")

    # Filter clinical_event based on max_adc_updt
#    filtered_clinical_event = (
#        spark.table("4_prod.raw.mill_dir_clinical_event")
#        .filter(F.col("ADC_UPDT") > max_adc_updt)
#        .filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp())
#    )

    filtered_clinical_event = (
    spark.table("4_prod.raw.mill_dir_clinical_event")
    .filter((F.year(F.col("UPDT_DT_TM")) > 1900) & (F.year(F.col("UPDT_DT_TM")) < 2009))
    .filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp())
    )

    # FormTemp
    form_temp = (
        spark.table("4_prod.raw.mill_dir_dcp_forms_activity")
        .join(
            spark.table("4_prod.raw.mill_dir_dcp_forms_ref"),
            "DCP_FORMS_REF_ID"
        )
        .join(
            filtered_clinical_event,
            (F.col("mill_dir_dcp_forms_activity.ENCNTR_ID") == filtered_clinical_event.ENCNTR_ID) &
            (F.col("mill_dir_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID").cast("integer") == 
             F.when(filtered_clinical_event.REFERENCE_NBR.contains("!"),
                    F.split(filtered_clinical_event.REFERENCE_NBR, "!").getItem(0).cast("float").cast("integer"))
             .otherwise(filtered_clinical_event.REFERENCE_NBR.cast("float").cast("integer"))),
            "left_semi"
        )
        .select(
            F.col("mill_dir_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID"),
            F.col("mill_dir_dcp_forms_activity.ENCNTR_ID"),
            F.col("mill_dir_dcp_forms_activity.FORM_STATUS_CD"),
            F.col("mill_dir_dcp_forms_ref.DCP_FORM_INSTANCE_ID"),
            F.greatest(F.col("mill_dir_dcp_forms_activity.ADC_UPDT"), F.col("mill_dir_dcp_forms_ref.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

    # DocResponseTemp
    doc_response_temp = (
        filtered_clinical_event.alias("CE")
        .join(form_temp, 
            (F.col("CE.ENCNTR_ID") == form_temp.ENCNTR_ID) &
            (F.when(F.col("CE.REFERENCE_NBR").contains("!"),
                    F.split(F.col("CE.REFERENCE_NBR"), "!").getItem(0).cast("float").cast("integer"))
             .otherwise(F.col("CE.REFERENCE_NBR").cast("float").cast("integer"))
             == form_temp.DCP_FORMS_ACTIVITY_ID.cast("integer")),
            "inner"
        )
        .join(spark.table("4_prod.raw.mill_dir_ce_date_result").alias("CEDR"), 
            F.col("CE.EVENT_ID") == F.col("CEDR.EVENT_ID"), "left")
        .join(spark.table("4_prod.raw.mill_dir_ce_string_result").alias("CESR"), 
            F.col("CE.EVENT_ID") == F.col("CESR.EVENT_ID"), "left")
        .join(spark.table("4_prod.raw.mill_dir_ce_coded_result").alias("CECR"), 
            F.col("CE.EVENT_ID") == F.col("CECR.EVENT_ID"), "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            (F.col("CECR.RESULT_CD") == F.col("CV.CODE_VALUE")) & (F.col("CV.ACTIVE_IND") == 1), "left")
        .join(filtered_clinical_event.alias("CE1"), 
            (F.col("CE.PARENT_EVENT_ID") == F.col("CE1.EVENT_ID")), "left")
        .filter(F.col("CE.TASK_ASSAY_CD") != 0)
        .select(
            F.col("CE.EVENT_ID").alias("DOC_RESPONSE_KEY"),
            F.col("CE.ENCNTR_ID"),
            F.col("CE.PERFORMED_DT_TM"),
            F.col("CE.EVENT_ID").alias("ELEMENT_EVENT_ID"),
            F.col("CE.PARENT_EVENT_ID").alias("SECTION_EVENT_ID"),
            F.col("CE.EVENT_ID").alias("FORM_EVENT_ID"),
            form_temp.DCP_FORM_INSTANCE_ID.alias("DOC_INPUT_ID"),
            form_temp.FORM_STATUS_CD,
            F.coalesce(F.col("CEDR.RESULT_DT_TM"), F.col("CESR.STRING_RESULT_TEXT"), F.col("CECR.DESCRIPTOR"), F.col("CV.DISPLAY")).alias("RESPONSE_VALUE_TXT"),
            F.when(F.col("CE1.EVENT_TITLE_TEXT").isin("TRACKING CONTROL", "Discrete Grid"), F.col("CE1.EVENT_ID")).otherwise("0").alias("GRID_EVENT_ID"),
            F.greatest(F.col("CE.ADC_UPDT"), form_temp.ADC_UPDT, F.coalesce(F.col("CEDR.ADC_UPDT"), F.col("CESR.ADC_UPDT"), F.col("CECR.ADC_UPDT"), F.col("CV.ADC_UPDT"), F.col("CE1.ADC_UPDT"))).alias("ADC_UPDT")
        )
    )

    # DocRefTemp
    doc_ref_temp = (
        spark.table("4_prod.raw.mill_dir_dcp_forms_ref").alias("FREF")
        .join(spark.table("4_prod.raw.mill_dir_dcp_forms_def").alias("FDEF"), 
            F.col("FREF.DCP_FORM_INSTANCE_ID") == F.col("FDEF.DCP_FORM_INSTANCE_ID"))
        .join(spark.table("4_prod.raw.mill_dir_dcp_section_ref").alias("SREF"), 
            F.col("FDEF.DCP_SECTION_REF_ID") == F.col("SREF.DCP_SECTION_REF_ID"))
        .join(spark.table("4_prod.raw.mill_dir_dcp_input_ref").alias("DIREF"), 
            F.col("SREF.DCP_SECTION_REF_ID") == F.col("DIREF.DCP_SECTION_REF_ID"))
        .join(spark.table("4_prod.raw.mill_dir_name_value_prefs").alias("NVP"), 
            (F.col("DIREF.DCP_INPUT_REF_ID") == F.col("NVP.PARENT_ENTITY_ID")) &
            (F.col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
            (F.col("NVP.PVC_NAME").like("discrete_task_assay%")), "left")
        .join(spark.table("4_prod.raw.mill_dir_discrete_task_assay").alias("DTA"), 
            F.col("NVP.MERGE_ID") == F.col("DTA.TASK_ASSAY_CD"), "left")
        .join(spark.table("4_prod.raw.mill_dir_name_value_prefs").alias("NVP2"), 
            (F.col("DIREF.DCP_INPUT_REF_ID") == F.col("NVP2.PARENT_ENTITY_ID")) &
            (F.col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
            (F.col("NVP2.PVC_NAME") == "grid_event_cd"), "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            (F.col("NVP2.MERGE_ID") == F.col("CV.CODE_VALUE")) & (F.col("CV.ACTIVE_IND") == 1), "left")
        .select(
            F.col("FREF.DCP_FORM_INSTANCE_ID").alias("DOC_INPUT_KEY"),
            F.col("FREF.DESCRIPTION").alias("FORM_DESC_TXT"),
            F.col("SREF.DESCRIPTION").alias("SECTION_DESC_TXT"),
            F.col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            F.col("CV.DISPLAY").alias("GRID_NAME_TXT"),
            F.col("DTA.DESCRIPTION").alias("GRID_COLUMN_DESC_TXT"),
            F.greatest(F.col("FREF.ADC_UPDT"), F.col("FDEF.ADC_UPDT"), F.col("SREF.ADC_UPDT"), F.col("DIREF.ADC_UPDT"), F.col("NVP.ADC_UPDT"), F.col("DTA.ADC_UPDT"), F.col("NVP2.ADC_UPDT"), F.col("CV.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

    return (
        doc_response_temp.alias("DRT")
        .join(dlt.read("rde_encounter").alias("ENC"), 
            doc_response_temp.ENCNTR_ID == F.col("ENC.ENCNTR_ID"))
        .join(doc_ref_temp.alias("FREF"), 
            doc_response_temp.DOC_INPUT_ID == doc_ref_temp.DOC_INPUT_KEY, "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            doc_response_temp.FORM_STATUS_CD == F.col("CV.CODE_VALUE"), "left")
        .select(
            F.col("ENC.NHS_Number").cast("string").alias("NHS_Number"),
            F.col("ENC.MRN").cast("string").alias("MRN"),
            F.col("ENC.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            F.col("ENC.ENCNTR_ID").cast("string").alias("ENCNTR_ID"), 
            F.date_format(F.col("PERFORMED_DT_TM"), "yyyy-MM-dd HH:mm").alias("PerformDate"),
            F.col("DOC_RESPONSE_KEY").cast("string").alias("DOC_RESPONSE_KEY"),
            F.col("FORM_DESC_TXT").cast("string").alias("Form"),
            F.col("FORM_EVENT_ID").cast("long").alias("FormID"),
            F.col("SECTION_DESC_TXT").cast("string").alias("Section"),
            F.col("SECTION_EVENT_ID").cast("long").alias("SectionID"),
            F.col("ELEMENT_LABEL_TXT").cast("string").alias("Element"),
            F.col("ELEMENT_EVENT_ID").cast("long").alias("ElementID"),
            F.col("GRID_NAME_TXT").cast("string").alias("Component"),
            F.col("GRID_COLUMN_DESC_TXT").cast("string").alias("ComponentDesc"),
            F.col("GRID_EVENT_ID").cast("long").alias("ComponentID"),
            F.col("RESPONSE_VALUE_TXT").cast("string").alias("Response"),
            F.when(F.col("RESPONSE_VALUE_TXT").rlike("^[0-9.]+$"), F.lit(1)).otherwise(F.lit(0)).cast("boolean").alias("ResponseNumeric"),
            F.col("CV.DESCRIPTION").cast("string").alias("Status"),
            F.greatest(
                F.col("ENC.ADC_UPDT"),
                F.col("DRT.ADC_UPDT"),
                F.col("FREF.ADC_UPDT"),
                F.col("CV.ADC_UPDT")).alias("ADC_UPDT")
        )
        #.filter(F.col("ADC_UPDT") > max_adc_updt)
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
    comment="Incrementally updated powerforms data",
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
