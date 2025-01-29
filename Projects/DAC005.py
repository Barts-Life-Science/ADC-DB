# Databricks notebook source
#Bloodcounts

project_identifier = 'dar005'

rde_tables = ['rde_all_procedures', 'rde_allergydetails', 'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_cds_apc', 'rde_cds_opa', 'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd', 'rde_family_history', 'rde_measurements', 'rde_medadmin', 'rde_op_diagnosis', 'rde_opa_opcs', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems', 'rde_pc_procedures', 'rde_pharmacyorders', 'rde_pathology', 'rde_raw_pathology']

bloodcount_tables = ['action_message', 'ana_internal1', 'ana_internal2', 'bb', 'bb_f', 'cal', 'cls_param', 'dst_data', 'dst_data_2', 'err', 'flag_sus2', 'flagging', 'flg_gate', 'giveup', 'hsa', 'hsa_f', 'ig', 'ipu_csv1', 'ipu_csv2', 'jo_aflas', 'jo_aflas01', 'jo_aflas03', 'jo_aflas04', 'lyact', 'mix_issue', 'mt_data', 'neonew_hpc', 'neuteoissue', 'otherinfo', 'outputdata', 'plt_abn_sus', 'plt_clumps_wnr_gate', 'plt_clumps_wnr_v17_18', 'plt_clumps_wnr_v21', 'plt_dst_raw_data', 'plt_swt', 'rbc_abn_sus', 'rbc_dst_raw_data', 'reportable', 'reportable_f', 'research', 'research_f', 'sampling_plt', 'sampling_pltf', 'sampling_rbc', 'sampling_ret', 'sampling_wdf', 'sampling_wdf_2times', 'sampling_wnr', 'sampling_wpc', 'sampling_wpc_2times', 'sampling_wpc_3times', 'sampling_wpc_4times', 'service_in', 'service_out', 'servicesettinglog', 't_data', 'wbc_abn_sct_bf', 'wbc_abn_sus', 'wdf_low_sfl', 'wnr_aggl', 'wp_reana', 'xn_sample']

max_ig_risk = 3
max_ig_severity = 2
columns_to_exclude = ['ADC_UPDT']


# Define the SNOMED codes and ICD10 code prefixes
snomed_codes = [
    # Glanzmanns thrombasthenia
    '32942005',
    
    # Inherited platelet disorder codes
    '768556005', '720520009', '720521008', '1187252002', '54569005', '782934004', 
    '725105006', '111396008', '1208617001', '441134009', '234476006', '1172901009', 
    '725291001', '234474009', '1172604004', '719021005', '46760003', '128102001', 
    '725034002', '720950009', '1285021005', '30575002', '234478007', '32942005', 
    '234471001', '234472008', '765977002', '51720005', '783255002', '128096008', 
    '1172699002', '783251006', '438492008', '9311003', '234469001', '128103006', 
    '1172685001', '718554005', '128100009', '774071007', '9417000', '128101008', 
    '234470000', '43346008', '51448009', '128099001', '128115005', '85589009', 
    '721882001', '723512008', '128098009', '1187614006', '771511005', '234475005', 
    '234477002', '733096007', '373420004', '718553004', '403837005', '36070007', 
    '719019000', '722475006', '782759001', '1156838007',
    
    # Blood coagulation disorder codes
    '359730001', '1196942001', '234466008', '25904003', '64509006', '439156006', 
    '191298004', '12153008', '35913006', '2036003', '14230004', '33820001', '35554008', 
    '15132005', '6364000', '234462005', '785308008', '359723007', '95843009', '282707003', 
    '1162807005', '1217410004', '1217409009', '725157006', '74576004', '234451005', 
    '278504009', '237337003', '1264141002', '1264140001', '234459007', '234465007', 
    '267199000', '237272003', '1177197005', '237271005', '237270006', '180481005', 
    '278366008', '278365007', '181456001', '36351005', '973271000000108', '973291000000107', 
    '307515009', '95842004', '128090002', '234460002', '782934004', '783194008', '725105006', 
    '64779008', '10749581000119100', '10749641000119106', '69500007', '123790009', 
    '123789000', '123787003', '123788008', '123786007', '128088003', '717936002', 
    '86075001', '234463000', '715559004', '154818001', '716746003', '234444001', 
    '234445000', '426199009', '234456000', '439145006', '717407006', '789291002', 
    '234452003', '10934005', '725291001', '1264233008', '1264232003', '191296000', 
    '191297009', '1148930004', '105604006', '51624005', '95839005', '67406007', 
    '39191000119103', '34417008', '16773005', '1148859000', '19307009', '111589005', 
    '95844003', '717935003', '773422002', '439458000', '58327003', '73975000', '767712006', 
    '89729000', '4320005', '41690001', '37193007', '234440005', '47307007', '76642003', 
    '1148862002', '9489006', '767713001', '88540000', '33169001', '45963004', '38879000', 
    '46981006', '18604004', '13993001', '27068000', '34478009', '6935003', '81783000', 
    '84048006', '65768009', '64315007', '66909001', '3760002', '61551003', '783256001', 
    '62751000119108', '234455001', '359727008', '237336007', '234464006', '1148857003', 
    '1148849001', '234461003', '95605009', '90935002', '782909004', '24149006', '12546009', 
    '73397007', '111588002', '439699000', '16922007', '439157002', '724356003', '1162804003', 
    '45366001', '95845002', '31925001', '33297000', '41788008', '438372000', '438792009', 
    '88776002', '40855001', '28293008', '234442002', '438360006', '37350004', '49762007', 
    '43217004', '439460003', '439455002', '439459008', '50189006', '440924009', '95841006', 
    '439274008', '439702007', '438827002', '1259242002', '128106003', '359700009', 
    '128113003', '128114009', '128107007', '359711001', '359717002', '359725000', 
    '359732009', '128108002', '441101007', '440988005', '27312002', '441188004', 
    '441189007', '439000005', '1177171004', '79674009', '234458004', '234457009', 
    '95840007', '307514008', '609456005', '609462000', '200173001', '724637001', 
    '86635005', '717937006', '19267009', '307518006', '1285665003', '440868005', 
    '1285666002', '441192006', '26029002', '1285655009', '425949001', '1285656005', 
    '440820004', '21360006', '50770000', '80988005', '1285663005', '440867000', 
    '1285664004', '441191004', '33344008', '1285659003', '438599002', '1285660008', 
    '441006000', '733028000', '95623001', '402851000', '76771005', '234453008', 
    '111452009', '49177006', '267272006', '200031006', '62410004', '16740451000119109', 
    '307517001', '48976006', '198912003', '61802005', '76407009', '1563006', '234454002', 
    '128115005', '13507004', '724854007', '371074009', '721304007', '128092005', 
    '41816006', '154826009', '1285661007', '440866009', '1285662000', '441190003', 
    '16872008', '1285657001', '438373005', '1285658006', '440993008', '10153004', 
    '699208000', '97571000119109', '30182008', '191322006', '49886003', '191323001', 
    '34395002', '307342006', '1197595004', '32605001', '416902009', '1156746003', 
    '1286003', '1148929009', '128105004', '41106001',
    
    # Myosin heavy chain 9 related
    '712922002'
]

# Define the ICD10 code prefixes
icd10_prefixes = [
    # VWD
    'D6800', 'D6801', 'D6802', 'D6803', 'D6804', 'D6809',
    # Haemophilia
    'D66', 'D67',
    # Rare Factor Deficiencies
    'D681',
    # Miscl (Acquired)
    'D682', 'D683', 'D684', 'D688', 'D669',
    # Platelet Disorders
    'D691', 'D6949', 'D6942', 'D693',
    # BDUC
    'D699', 'D689', 'D698', 'I78'
]

# Create the cohort view SQL
cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
WITH nomenclature_filter AS (
    SELECT NOMENCLATURE_ID
    FROM 4_prod.bronze.nomenclature
    WHERE (SNOMED_TYPE = 'NATIVE' AND SNOMED_CODE IN ({','.join(f"'{code}'" for code in snomed_codes)}))
    OR (ICD10_TYPE = 'NATIVE' AND ({' OR '.join(f"ICD10_CODE LIKE '{prefix}%'" for prefix in icd10_prefixes)}))
),
matching_patients AS (
    SELECT DISTINCT PERSON_ID
    FROM (
        SELECT PERSON_ID, NOMENCLATURE_ID
        FROM 4_prod.raw.mill_diagnosis
        UNION
        SELECT PERSON_ID, NOMENCLATURE_ID
        FROM 4_prod.raw.mill_problem
    ) diagnoses
    WHERE NOMENCLATURE_ID IN (SELECT NOMENCLATURE_ID FROM nomenclature_filter)
)
SELECT 
    DISTINCT mp.PERSON_ID AS PERSON_ID,
    CAST(NULL AS STRING) AS SUBCOHORT
FROM matching_patients mp
"""

# Execute the SQL to create the view
spark.sql(cohort_sql)



spark.sql("USE CATALOG 5_projects")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS 5_projects.{project_identifier}")

# Get list of existing views in the target schema
existing_views_df = spark.sql(f"""
    SHOW VIEWS IN 5_projects.{project_identifier}
""")

# Drop all existing views in the schema
if existing_views_df.count() > 0:
    for row in existing_views_df.collect():
        view_name = row.viewName
        spark.sql(f"DROP VIEW IF EXISTS {project_identifier}.{view_name}")
        print(f"Dropped view: {project_identifier}.{view_name}")

def get_sample_number_column(table_name):
    columns = spark.table(f"4_prod.ancil_bloodcounts.{table_name}").columns
    if 'sample_no_' in columns:
        return 'sample_no_'
    elif 'sampleid' in columns:
        return 'sampleid'
    else:
        return None

def get_columns_with_high_tags(table_name):
    # Get columns with high ig_risk
    high_risk_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = 'rde'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_risk'
        AND tag_value > {max_ig_risk}
    """).toPandas()['column_name'].tolist()

    # Get columns with high ig_severity
    high_severity_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = 'rde'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_severity'
        AND tag_value > {max_ig_severity}
    """).toPandas()['column_name'].tolist()

    # Convert the combined list to a set before returning
    return high_risk_columns + high_severity_columns

# Function to get column names excluding specified columns and columns with high tags
def get_columns_except_excluded(table_name):
    # Get all columns from the table
    all_columns = spark.table(f"4_prod.rde.{table_name}").columns
    
    # Get columns with high risk or severity tags
    high_tag_columns = get_columns_with_high_tags(table_name)

    all_exluded_columns = high_tag_columns + columns_to_exclude
    
    # Filter out excluded columns using set difference
    filtered_columns = list(set(all_columns) - set(all_exluded_columns))
    
    # Convert back to sorted list and join
    return ", ".join(sorted(filtered_columns))

# Function to determine the person ID column name
def get_person_id_column(table_name):
    columns = spark.table(f"4_prod.rde.{table_name}").columns
    if 'PERSON_ID' in columns:
        return 'PERSON_ID'
    elif 'PERSONID' in columns:
        return 'PERSONID'
    elif 'Person_ID' in columns:
        return 'Person_ID'
    else:
        return None

for table in rde_tables:
    # Get columns string
    columns = get_columns_except_excluded(table)
    
    # Get the appropriate person ID column name
    person_id_col = get_person_id_column(table)
    
    if person_id_col:
        # Create view SQL with cohort filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        WITH source_data AS (
            SELECT {columns}
            FROM 4_prod.rde.{table}
        )
        SELECT s.*
        FROM source_data s
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c
        ON s.{person_id_col} = c.PERSON_ID
        """
    else:
        # If no person ID column exists, create view without filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT {columns}
        FROM 4_prod.rde.{table}
        """
        print(f"Warning: No person ID column found in {table}. Creating view without cohort filtering.")
    
    # Execute the SQL
    spark.sql(view_sql)
    
    print(f"Created view: 5_projects.{project_identifier}.{table}")

# Process bloodcount tables
for table in bloodcount_tables:
    # Get the appropriate sample number column name
    sample_no_col = get_sample_number_column(table)
    
    if sample_no_col:
        # Create view SQL with cohort filtering through rde_raw_pathology
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT b.*
        FROM 4_prod.ancil_bloodcounts.{table} b
        INNER JOIN 4_prod.rde.rde_raw_pathology p ON b.{sample_no_col} = p.LabNo
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c ON p.PERSON_ID = c.PERSON_ID
        """
    
    # Execute the SQL
    spark.sql(view_sql)
    
    print(f"Created view: 5_projects.{project_identifier}.{table}")


# Create schema view
schema_sql = f"""
CREATE OR REPLACE VIEW 5_projects.{project_identifier}.schema AS
SELECT 
    table_name,
    column_name,
    COALESCE(comment, '') as column_comment
FROM 5_projects.information_schema.columns
WHERE table_catalog = '5_projects'
AND table_schema = '{project_identifier}'
AND table_name != 'schema'
ORDER BY table_name, column_name
"""
spark.sql(schema_sql)
print(f"Created schema view: 5_projects.{project_identifier}.schema")
