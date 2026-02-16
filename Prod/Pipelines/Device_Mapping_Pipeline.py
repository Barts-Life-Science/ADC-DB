# Databricks notebook source
# MAGIC %md
# MAGIC # Device Mapping Pipeline (Incremental)
# MAGIC
# MAGIC Optimized incremental version - processes only new records, updates anchors, runs all layers.
# MAGIC 5-layer approach matching the full pipeline. Serverless compatible.
# MAGIC
# MAGIC Features:
# MAGIC - **Full GMDN embedding comparison** (compares against ALL 13k GMDN terms)
# MAGIC - Embedding caching (shared with full pipeline)
# MAGIC - **Expanded brand/term lookup** for common abbreviations and brand names
# MAGIC - Procedure-aware thresholds
# MAGIC - Handles new GMDN terms added to reference table
# MAGIC - **False positive prevention** (GMDN exclusions, material/body part conflict detection)
# MAGIC - **Cache invalidation** for stale entries (brand overrides + FP exclusions)
# MAGIC - **SNOMED enrichment** via lookup table + manual curated mappings
# MAGIC - **Confidence tiers** (HIGH/MEDIUM/LOW) for downstream OMOP filtering

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType, DoubleType, ArrayType, StructType, StructField
import re
import numpy as np
import time
from openai import OpenAI
from datetime import datetime

# COMMAND ----------

CONFIG = {
    "implant_details": "4_prod.bronze.map_implant_details",
    "gudid_device": "3_lookup.devices.device",
    "gudid_gmdnterms": "3_lookup.devices.gmdnterms",
    "snomed_concept": "4_prod.omop.concept",
    "concept_relationship": "4_prod.omop.concept_relationship",
    "concept_ancestor": "4_prod.omop.concept_ancestor",
    "procedures": "4_prod.rde.rde_all_procedures",
    "anchor_lookup": "3_lookup.device_mapping.anchor_lookup",
    "opcs_device_lookup": "3_lookup.device_mapping.opcs_to_device",
    "gmdn_snomed_lookup": "3_lookup.device_mapping.gmdn_to_snomed",
    "device_mapping_output": "4_prod.bronze.map_device_mapping",
    "embedding_cache": "3_lookup.device_mapping.embedding_cache",
    "layer5_matches": "3_lookup.device_mapping.layer5_match_cache",
}

EMBEDDING_SIMILARITY_THRESHOLD = 0.7
DEFAULT_LAYER5_THRESHOLD = 0.65
PROCEDURE_BOOSTED_THRESHOLD = 0.55

# Manually curated GMDN->SNOMED mappings for codes not covered by the automated lookup
MANUAL_SNOMED_MAPPINGS = {
    36093: (4239189, 'Bone screw'),
    47279: (4098108, 'Bone plate'),
    41888: (40355609, 'Intrauterine contraceptive device'),
    36566: (45768044, 'Surgical mesh'),
    35506: (4245414, 'Bone cement'),
    47337: (45768074, 'Knee tibia prosthesis'),
    36182: (4156227, 'Pericardial patch'),
    36611: (45759677, 'Tibial insert'),
    38630: (4231009, 'Cardiac pacemaker electrode'),
    61067: (4158970, 'Glaucoma drainage device'),
    38631: (45772840, 'Implantable cardiac pacemaker'),
    61669: (4300890, 'Bone staple'),
    38482: (45761793, 'Acetabular shell'),
    47331: (45772834, 'Acetabular liner'),
}

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
EMBEDDING_MODEL = "azure_openai_embedding_endpoint"
WORKSPACE_URL = "https://adb-3660342888273328.8.azuredatabricks.net/serving-endpoints"
embedding_client = OpenAI(api_key=DATABRICKS_TOKEN, base_url=WORKSPACE_URL)

OUTPUT_SCHEMA = {
    "EVENT_ID": LongType(), "ENCNTR_ID": LongType(), "PERSON_ID": LongType(),
    "IMPLANT_DESCRIPTION": StringType(), "gmdncode": LongType(), "gmdn_name": StringType(),
    "snomed_concept_id": LongType(), "snomed_name": StringType(), "device_type": StringType(),
    "mapping_layer": StringType(), "mapping_confidence": DoubleType()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expanded Brand/Term Lookup
# MAGIC
# MAGIC Handles brand names, abbreviations, and generic terms that embed poorly.
# MAGIC Order matters: more specific terms should come BEFORE generic ones.

# COMMAND ----------

# Brand name -> GMDN mappings for common abbreviations and brand names
BRAND_GMDN_LOOKUP = {
    # ===========================================
    # INTRAOCULAR LENSES (IOL) - Code 35658
    # Most common false positive category
    # ===========================================
    # Specific brand/model names first
    'eyecee one crystal': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'eyecee one': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'eyecee iol': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'eyecee': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'rayner one': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'ray one': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'rayone': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'rayner cflex': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'rayner c flex': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'rayner': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'c-flex': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'c flex': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'cflex': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'envista': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'envist': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),  # Typo variant
    'tecnis': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'acrysof': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'clareon': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'softport': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'sofport': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'versario': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'carlevale': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'carlevalie': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),  # Typo variant
    'hoya': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'zeiss': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'bausch': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'alcon': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    # Generic IOL terms (after brands)
    'preloaded lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'pre loaded lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'pre-loaded lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'aspheric lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'intraocular lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'intra ocular lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'intra-ocular lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'eye lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'io lens': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'pc iol': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'pciol': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    'iol': (35658, 'Posterior-chamber intraocular lens, pseudophakic'),
    
    # ===========================================
    # INTRAUTERINE DEVICES (IUD) - Code 41888
    # ===========================================
    'mirena coil': (41888, 'Intrauterine contraceptive device, medicated'),
    'mirena iud': (41888, 'Intrauterine contraceptive device, medicated'),
    'mirena intrauterine': (41888, 'Intrauterine contraceptive device, medicated'),
    'mirena': (41888, 'Intrauterine contraceptive device, medicated'),
    'kyleena': (41888, 'Intrauterine contraceptive device, medicated'),
    'jaydess': (41888, 'Intrauterine contraceptive device, medicated'),
    'levosert': (41888, 'Intrauterine contraceptive device, medicated'),
    'iud': (41888, 'Intrauterine contraceptive device, medicated'),
    'ius': (41888, 'Intrauterine contraceptive device, medicated'),
    # Contraceptive implants (arm)
    'nexplanon': (47654, 'Implantable contraceptive hormone delivery system'),
    'implanon': (47654, 'Implantable contraceptive hormone delivery system'),
    # Copper IUD / non-medicated (using 41888 as closest GMDN)
    't-safe': (41888, 'Intrauterine contraceptive device, medicated'),
    't safe': (41888, 'Intrauterine contraceptive device, medicated'),
    'copper coil': (41888, 'Intrauterine contraceptive device, medicated'),
    'copper t': (41888, 'Intrauterine contraceptive device, medicated'),
    'intrauterine device': (41888, 'Intrauterine contraceptive device, medicated'),
    'intrauterine contraceptive': (41888, 'Intrauterine contraceptive device, medicated'),
    
    # ===========================================
    # ORTHOPAEDIC - WIRES & PINS - Code 35685
    # ===========================================
    'kirschner wire': (35685, 'Orthopaedic bone wire'),
    'kirschner-wire': (35685, 'Orthopaedic bone wire'),
    'k-wire': (35685, 'Orthopaedic bone wire'),
    'k wire': (35685, 'Orthopaedic bone wire'),
    'kwire': (35685, 'Orthopaedic bone wire'),
    'steinmann pin': (35685, 'Orthopaedic bone wire'),
    
    # ===========================================
    # ORTHOPAEDIC - SCREWS - Code 36093
    # ===========================================
    'cortex screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'cortical screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'cancellous screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'lock screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'locking screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'va lock screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'maxdrive': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'stardrive screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'torx screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'mf cortex': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'mandible cortex': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    'mandible screw': (36093, 'Orthopaedic bone screw, non-bioabsorbable'),
    
    # ===========================================
    # ORTHOPAEDIC - DHS/DCS (Hip Screws) - Code 66943
    # ===========================================
    'dynamic hip screw': (66943, 'Femoral fixation plate/blade'),
    'dynamic condylar screw': (66943, 'Femoral fixation plate/blade'),
    'dhs/dcs': (66943, 'Femoral fixation plate/blade'),
    'dhs dcs': (66943, 'Femoral fixation plate/blade'),
    'dhs screw': (66943, 'Femoral fixation plate/blade'),
    'dcs screw': (66943, 'Femoral fixation plate/blade'),
    'dhs': (66943, 'Femoral fixation plate/blade'),
    
    # ===========================================
    # ORTHOPAEDIC - PLATES - Code 47279
    # ===========================================
    'lcp plate': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'lcp 3.5': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'lcp 3 5': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'lcp': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'mandible plate': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'recon plate': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'reconstruction plate': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'mini plate': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'matrix midface': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    'matrix neuro': (47279, 'Orthopaedic bone fixation plate, non-bioabsorbable'),
    
    # ===========================================
    # ORTHOPAEDIC - NAILS - Code 33187
    # ===========================================
    'tfna': (33187, 'Femur nail'),
    'gamma nail': (33187, 'Femur nail'),
    'gamma 3': (33187, 'Femur nail'),
    'pfna': (33187, 'Femur nail'),
    'femoral nail': (33187, 'Femur nail'),
    'femur nail': (33187, 'Femur nail'),
    'helical blade': (33187, 'Femur nail'),
    # Tibia nail - Code 38152
    'tibia nail': (38152, 'Tibia nail'),
    'tibial nail': (38152, 'Tibia nail'),
    
    # ===========================================
    # ORTHOPAEDIC - HIP COMPONENTS
    # ===========================================
    # Ceramic heads - Code 38156
    'biolox delta': (38156, 'Ceramic femoral head prosthesis'),
    'biolox': (38156, 'Ceramic femoral head prosthesis'),
    'ceramic head': (38156, 'Ceramic femoral head prosthesis'),
    'delta ceramic': (38156, 'Ceramic femoral head prosthesis'),
    
    # Acetabular cups/liners - Code 47496
    'delta cup': (47496, 'Ceramic acetabular cup prosthesis'),
    'ceramic liner': (47496, 'Ceramic acetabular cup prosthesis'),
    'ceramic cup': (47496, 'Ceramic acetabular cup prosthesis'),
    
    # Polyethylene liners - Code 36777
    'poly liner': (36777, 'Acetabular cup prosthesis liner'),
    'polyethylene liner': (36777, 'Acetabular cup prosthesis liner'),
    'pe liner': (36777, 'Acetabular cup prosthesis liner'),
    
    # ===========================================
    # ORTHOPAEDIC - KNEE COMPONENTS
    # ===========================================
    # Tibial components - Code 47501
    'tibial tray': (47501, 'Knee joint tibial prosthesis'),
    'tibial baseplate': (47501, 'Knee joint tibial prosthesis'),
    'tibial component': (47501, 'Knee joint tibial prosthesis'),
    'tibial plate': (47501, 'Knee joint tibial prosthesis'),
    'attune tibial': (47501, 'Knee joint tibial prosthesis'),
    'triathlon tibial': (47501, 'Knee joint tibial prosthesis'),
    'persona tibial': (47501, 'Knee joint tibial prosthesis'),
    
    # ===========================================
    # BONE CEMENT - Code 35506
    # ===========================================
    'palacos r+g': (35506, 'Orthopaedic bone cement'),
    'palacos r g': (35506, 'Orthopaedic bone cement'),
    'palacos mv+g': (35506, 'Orthopaedic bone cement'),
    'palacos mv g': (35506, 'Orthopaedic bone cement'),
    'palacos': (35506, 'Orthopaedic bone cement'),
    'simplex': (35506, 'Orthopaedic bone cement'),
    'smartset': (35506, 'Orthopaedic bone cement'),
    'cmw': (35506, 'Orthopaedic bone cement'),
    'bone cement': (35506, 'Orthopaedic bone cement'),
    # Cement restrictor - Code 38073
    'cement restrictor': (38073, 'Metallic orthopaedic cement restrictor'),
    'hardinge': (38073, 'Metallic orthopaedic cement restrictor'),
    
    # ===========================================
    # VASCULAR PATCHES - Codes 35273 (biologic), 31744 (synthetic)
    # ===========================================
    'xenosure': (35273, 'Cardiovascular patch, animal-derived'),
    'xeno sure': (35273, 'Cardiovascular patch, animal-derived'),
    'biologic patch': (35273, 'Cardiovascular patch, animal-derived'),
    'biological patch': (35273, 'Cardiovascular patch, animal-derived'),
    # Pericardium / tissue patch - Code 36182
    'peri-guard': (36182, 'Pericardium prosthesis'),
    'peri guard': (36182, 'Pericardium prosthesis'),
    'supple peri': (36182, 'Pericardium prosthesis'),
    'tutoplast': (36182, 'Pericardium prosthesis'),
    
    # ===========================================
    # VASCULAR GRAFTS - Code 35281
    # ===========================================
    'gelweave': (35281, 'Synthetic vascular graft'),
    'gel weave': (35281, 'Synthetic vascular graft'),
    'dacron graft': (35281, 'Synthetic vascular graft'),
    'vascular prosthesis': (35281, 'Synthetic vascular graft'),
    'vascular graft': (35281, 'Synthetic vascular graft'),
    'vascutek': (35281, 'Synthetic vascular graft'),
    'gelatin impregnated': (35281, 'Synthetic vascular graft'),
    
    # ===========================================
    # HERNIA MESH - Code 36566
    # ===========================================
    'parietex': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    'prolene mesh': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    'ultrapro mesh': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    'hernia mesh': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    'surgical mesh': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    'progrip': (36566, 'Surgical mesh, synthetic, non-bioabsorbable'),
    
    # ===========================================
    # CARDIAC DEVICES
    # ===========================================
    # Pacemaker leads - Code 38630
    'ingevity': (38630, 'Implantable cardiac pacemaker electrode'),
    'tendril': (38630, 'Implantable cardiac pacemaker electrode'),
    'pacemaker lead': (38630, 'Implantable cardiac pacemaker electrode'),
    
    # Pacemaker generators - Code 38472
    'advisa': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    'ensura': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    'azure': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    'accolade': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    'proponent': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    'pacemaker generator': (38472, 'Implantable cardiac pacemaker, dual-chamber'),
    
    # ICDs/CRT-D - Code 47270
    'emblem': (47270, 'Cardiac resynchronization therapy implantable defibrillator'),
    's-icd': (47270, 'Cardiac resynchronization therapy implantable defibrillator'),
    'resonate': (47270, 'Cardiac resynchronization therapy implantable defibrillator'),
    'crt-d': (47270, 'Cardiac resynchronization therapy implantable defibrillator'),
    'crt d': (47270, 'Cardiac resynchronization therapy implantable defibrillator'),
    'tyrx': (62026, 'Cardiac implantable electronic device antibacterial envelope'),
    
    # Heart valves - Code 60242 (aortic) / 60244 (mitral)
    'perimount magna mitral': (60244, 'Mitral heart valve bioprosthesis'),
    'perimount mitral': (60244, 'Mitral heart valve bioprosthesis'),
    'perimount magna ease aortic': (60242, 'Aortic heart valve bioprosthesis'),
    'perimount magna ease': (60242, 'Aortic heart valve bioprosthesis'),
    'perimount magna': (60242, 'Aortic heart valve bioprosthesis'),
    'perimount aortic': (60242, 'Aortic heart valve bioprosthesis'),
    'perimount': (60242, 'Aortic heart valve bioprosthesis'),
    
    # ===========================================
    # GLAUCOMA DEVICES - Code 61067
    # ===========================================
    'istent': (61067, 'Glaucoma drainage device'),
    'i-stent': (61067, 'Glaucoma drainage device'),
    'xen implant': (61067, 'Glaucoma drainage device'),
    'xen': (61067, 'Glaucoma drainage device'),
    
    # ===========================================
    # CAPSULAR TENSION RING - Code 42839
    # ===========================================
    'capsular tension ring': (42839, 'Capsular tension ring'),
    'tension ring': (42839, 'Capsular tension ring'),
    
    # ===========================================
    # STAPES / EAR PROSTHESIS - Code 35690
    # ===========================================
    'stapes piston': (35690, 'Ossicular prosthesis, partial'),
    'smart stapes': (35690, 'Ossicular prosthesis, partial'),
    'stapes prosthesis': (35690, 'Ossicular prosthesis, partial'),
    
    # ===========================================
    # BONE STAPLE - Code 61669
    # ===========================================
    'static staple': (61669, 'Orthopaedic bone staple, non-adjustable'),
    'bone staple': (61669, 'Orthopaedic bone staple, non-adjustable'),
    
    # ===========================================
    # PECTUS - Code 61522
    # ===========================================
    'pectus bar': (61522, 'Funnel chest remodelling bar'),
    'nuss bar': (61522, 'Funnel chest remodelling bar'),
    'nuss': (61522, 'Funnel chest remodelling bar'),
    
    # ===========================================
    # BREAST IMPLANTS - Code 36197
    # ===========================================
    'breast implant': (36197, 'Silicone gel-filled breast implant'),
    'mentor': (36197, 'Silicone gel-filled breast implant'),
    'natrelle': (36197, 'Silicone gel-filled breast implant'),
    'siltex': (36197, 'Silicone gel-filled breast implant'),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1c. False Positive Prevention
# MAGIC
# MAGIC GMDN terms that are surgical instruments, not implants, and should never be matched.
# MAGIC Material and body part conflicts that indicate a false positive match.

# COMMAND ----------

# GMDN terms that should never be matched (surgical instruments, not implants)
GMDN_EXCLUSIONS = {
    'lens spoon',
    'lens forceps',
    'vascular graft tunneller',
    'pre-prescription spectacle lens',
    'manual intraocular lens injector',
    'cardiac pulse generator programmer',
    'humeral head spacer',
    'bipolar humeral head',
    'middle ear drill bit',
    'arthrodesis nail',
    'intraocular lens folder',                  # IOL insertion tool, not the lens
    'intraocular lens calculation software',     # Software, not a device
    'intraocular lens implantation cord',        # IOL accessory, not the lens
    'implantable cardiac monitor',               # Monitor, not a defibrillator/pacemaker
}

# Material conflicts: if description has key material, GMDN should NOT have value materials
MATERIAL_CONFLICTS = {
    'polyethylene': ['ceramic'],
    'poly': ['ceramic'],
    'titanium': ['ceramic'],
    'metal': ['ceramic'],
    'cocrmo': ['ceramic'],
    'cobalt': ['ceramic'],
    'stainless': ['ceramic'],
}

# Body part conflicts: if description mentions key body part, GMDN should NOT mention value body parts
BODY_PART_CONFLICTS = {
    'tibial': ['ankle', 'humeral', 'shoulder'],
    'knee': ['ankle', 'shoulder', 'hip'],
    'hip': ['ankle', 'shoulder', 'knee', 'humeral'],
    'acetabular': ['humeral', 'shoulder', 'glenoid'],
    'femoral': ['humeral', 'shoulder', 'glenoid'],
}

def is_valid_match(desc, gmdn_name):
    """Check if a match is valid (not a likely false positive)."""
    if not desc or not gmdn_name:
        return True
    
    desc_lower = desc.lower()
    gmdn_lower = gmdn_name.lower()
    
    # Check GMDN exclusions
    for excluded in GMDN_EXCLUSIONS:
        if excluded in gmdn_lower:
            return False
    
    # Check material conflicts
    for desc_material, gmdn_materials in MATERIAL_CONFLICTS.items():
        if desc_material in desc_lower:
            for gmdn_material in gmdn_materials:
                if gmdn_material in gmdn_lower:
                    return False
    
    # Check body part conflicts
    for desc_part, gmdn_parts in BODY_PART_CONFLICTS.items():
        if desc_part in desc_lower:
            for gmdn_part in gmdn_parts:
                if gmdn_part in gmdn_lower:
                    return False
    
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Procedure Categories for Boosting

# COMMAND ----------

PROCEDURE_DEVICE_CATEGORIES = {
    'W37': ['hip', 'femoral', 'acetabular', 'prosthesis'],
    'W38': ['hip', 'femoral', 'acetabular', 'prosthesis'],
    'W39': ['hip', 'femoral', 'acetabular', 'prosthesis'],
    'W40': ['knee', 'tibial', 'femoral', 'patella', 'prosthesis'],
    'W41': ['knee', 'tibial', 'femoral', 'patella', 'prosthesis'],
    'W42': ['knee', 'tibial', 'femoral', 'patella', 'prosthesis'],
    'W19': ['nail', 'screw', 'plate', 'wire', 'pin'],
    'W20': ['nail', 'screw', 'plate', 'wire', 'pin'],
    'K60': ['pacemaker', 'lead', 'generator', 'cardiac'],
    'K61': ['pacemaker', 'lead', 'generator', 'cardiac'],
    'K59': ['defibrillator', 'icd', 'lead', 'cardiac'],
    'C71': ['lens', 'intraocular', 'iol', 'posterior', 'anterior'],
    'C72': ['lens', 'intraocular', 'iol', 'posterior', 'anterior'],
    'C75': ['lens', 'intraocular', 'iol', 'posterior', 'anterior'],
    'K40': ['stent', 'graft', 'coronary'],
    'K41': ['stent', 'graft', 'coronary'],
    'L29': ['stent', 'graft', 'vascular', 'aortic'],
    'T20': ['mesh', 'hernia', 'patch'],
    'T21': ['mesh', 'hernia', 'patch'],
    'T22': ['mesh', 'hernia', 'patch'],
    'T24': ['mesh', 'hernia', 'patch'],
    'T27': ['mesh', 'hernia', 'patch'],
    'T06': ['pectus', 'chest', 'funnel', 'bar'],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilities

# COMMAND ----------

# Global embedding cache
_embedding_cache = {}
_embedding_cache_loaded = False
_new_embeddings = []

def load_embedding_cache():
    global _embedding_cache, _embedding_cache_loaded
    if _embedding_cache_loaded:
        return
    try:
        # Only load layer5 embeddings (for description matching in Layer 5)
        # GMDN embeddings are loaded via Spark-side join in load_gmdn_embedding_matrix()
        rows = spark.table(CONFIG["embedding_cache"]).filter(F.col("text_type") == "layer5").collect()
        for r in rows:
            _embedding_cache[(r.text, r.text_type)] = list(r.embedding)
        print(f"    Loaded {len(_embedding_cache):,} cached layer5 embeddings")
    except Exception as e:
        print(f"    No embedding cache: {e}")
    _embedding_cache_loaded = True

def save_new_embeddings():
    global _new_embeddings
    if not _new_embeddings:
        return
    schema = StructType([
        StructField("text", StringType()),
        StructField("text_type", StringType()),
        StructField("embedding", ArrayType(DoubleType())),
        StructField("created_at", StringType())
    ])
    df = spark.createDataFrame(_new_embeddings, schema=schema)
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    df.write.mode("append").saveAsTable(CONFIG["embedding_cache"])
    _new_embeddings = []

def get_embeddings_batch_cached(texts, text_type, batch_size=100):
    global _new_embeddings
    load_embedding_cache()
    
    results = [None] * len(texts)
    texts_to_fetch = []
    indices_to_fetch = []
    
    for i, text in enumerate(texts):
        cached = _embedding_cache.get((text, text_type))
        if cached is not None:
            results[i] = cached
        else:
            texts_to_fetch.append(text)
            indices_to_fetch.append(i)
    
    if texts_to_fetch:
        now = datetime.now().isoformat()
        for batch_start in range(0, len(texts_to_fetch), batch_size):
            batch = texts_to_fetch[batch_start:batch_start + batch_size]
            batch_indices = indices_to_fetch[batch_start:batch_start + batch_size]
            try:
                resp = embedding_client.embeddings.create(model=EMBEDDING_MODEL, input=batch, encoding_format="float")
                for j, item in enumerate(resp.data):
                    emb = item.embedding
                    idx = batch_indices[j]
                    results[idx] = emb
                    text = batch[j]
                    _embedding_cache[(text, text_type)] = emb
                    _new_embeddings.append({
                        'text': text, 'text_type': text_type,
                        'embedding': [float(x) for x in emb], 'created_at': now
                    })
            except Exception as e:
                print(f"[WARN] Embedding error: {e}")
            if batch_start + batch_size < len(texts_to_fetch):
                time.sleep(0.1)
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## GMDN Embedding Matrix (Full Comparison with False Positive Validation)

# COMMAND ----------

# Global GMDN embedding matrix
_gmdn_matrix = None
_gmdn_codes = None
_gmdn_names = None
_gmdn_norms = None

def ensure_gmdn_embeddings_cached():
    """Pre-embed all GMDN terms if not already cached."""
    load_embedding_cache()
    
    gmdn_rows = spark.sql("""
        SELECT CAST(gmdncode AS BIGINT) as gmdncode, FIRST(gmdnptname) as gmdn_name
        FROM {gudid_gmdnterms}
        WHERE gmdncode IS NOT NULL AND gmdnptname IS NOT NULL
        GROUP BY gmdncode
    """.format(**CONFIG)).collect()
    
    gmdn_terms = [(r.gmdncode, r.gmdn_name) for r in gmdn_rows]
    print(f"    Total GMDN terms: {len(gmdn_terms):,}")
    
    uncached = [(code, name) for code, name in gmdn_terms if (name, 'gmdn') not in _embedding_cache]
    print(f"    Already cached: {len(gmdn_terms) - len(uncached):,}")
    print(f"    Need to embed: {len(uncached):,}")
    
    if uncached:
        print(f"    Embedding {len(uncached):,} new GMDN terms...")
        batch_size = 100
        for i in range(0, len(uncached), batch_size):
            batch = uncached[i:i + batch_size]
            texts = [name for _, name in batch]
            get_embeddings_batch_cached(texts, text_type='gmdn', batch_size=batch_size)
            if (i + batch_size) % 1000 == 0:
                print(f"      Processed {min(i + batch_size, len(uncached)):,}/{len(uncached):,}")
                save_new_embeddings()
        save_new_embeddings()
        print(f"    Done embedding GMDN terms.")
    
    return gmdn_terms

def load_gmdn_embedding_matrix():
    """Load all GMDN embeddings into a numpy matrix for fast comparison.
    
    Uses Spark-side join to load only GMDN embeddings (~13K rows) instead of
    collecting the entire embedding cache (~80K+ rows) into driver memory.
    """
    global _gmdn_matrix, _gmdn_codes, _gmdn_names, _gmdn_norms
    
    if _gmdn_matrix is not None:
        return _gmdn_codes, _gmdn_names, _gmdn_matrix, _gmdn_norms
    
    print(f"    Building GMDN embedding matrix (Spark-side join)...")
    
    gmdn_terms = spark.sql("""
        SELECT CAST(gmdncode AS BIGINT) as gmdncode, FIRST(gmdnptname) as gmdn_name
        FROM {gudid_gmdnterms}
        WHERE gmdncode IS NOT NULL AND gmdnptname IS NOT NULL
        GROUP BY gmdncode
    """.format(**CONFIG))
    
    cached = spark.table(CONFIG["embedding_cache"]).filter(F.col("text_type") == "gmdn")
    
    joined = (gmdn_terms.join(cached, gmdn_terms.gmdn_name == cached.text, "inner")
        .select("gmdncode", "gmdn_name", "embedding").collect())
    
    if not joined:
        print("    WARNING: No GMDN embeddings in cache - falling back to full cache load")
        gmdn_terms_list = ensure_gmdn_embeddings_cached()
        codes, names, embeddings = [], [], []
        for code, name in gmdn_terms_list:
            emb = _embedding_cache.get((name, 'gmdn'))
            if emb is not None:
                codes.append(code)
                names.append(name)
                embeddings.append(emb)
    else:
        codes = [int(r.gmdncode) for r in joined]
        names = [r.gmdn_name for r in joined]
        embeddings = [list(r.embedding) for r in joined]
    
    _gmdn_codes = np.array(codes)
    _gmdn_names = np.array(names)
    _gmdn_matrix = np.array(embeddings)
    _gmdn_norms = np.linalg.norm(_gmdn_matrix, axis=1)
    
    print(f"    GMDN matrix shape: {_gmdn_matrix.shape}")
    print(f"    Ready for full comparison against {len(_gmdn_codes):,} GMDN terms")
    
    return _gmdn_codes, _gmdn_names, _gmdn_matrix, _gmdn_norms

def find_best_gmdn_match(desc_embedding, desc_text=None):
    """Find best GMDN match using full matrix comparison with false positive validation.
    
    Returns top 10 matches by similarity, then validates each in order to avoid
    false positives. Returns the first valid match.
    """
    global _gmdn_matrix, _gmdn_codes, _gmdn_names, _gmdn_norms
    
    if _gmdn_matrix is None:
        load_gmdn_embedding_matrix()
    
    desc_vec = np.array(desc_embedding)
    desc_norm = np.linalg.norm(desc_vec)
    
    if desc_norm == 0:
        return None, None, 0.0, False
    
    similarities = np.dot(_gmdn_matrix, desc_vec) / (_gmdn_norms * desc_norm)
    
    # Get top 10 matches
    top_indices = np.argsort(similarities)[-10:][::-1]
    
    # Check each match for validity
    for idx in top_indices:
        sim = float(similarities[idx])
        code = int(_gmdn_codes[idx])
        name = str(_gmdn_names[idx])
        
        # If no description text provided, return best match without validation
        if desc_text is None:
            return code, name, sim, False
        
        # Validate the match
        if is_valid_match(desc_text, name):
            return code, name, sim, False
    
    # If all top 10 were rejected, return the best with rejected flag
    best_idx = top_indices[0]
    return (int(_gmdn_codes[best_idx]), str(_gmdn_names[best_idx]), 
            float(similarities[best_idx]), True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Utilities

# COMMAND ----------

def get_manufacturer_names():
    rows = spark.sql("SELECT DISTINCT LOWER(companyname) as name FROM {gudid_device} WHERE companyname IS NOT NULL AND LENGTH(companyname) >= 4".format(**CONFIG)).collect()
    return [row.name for row in rows]

def create_text_cleaning_udf(manufacturers):
    mfr_set = set(manufacturers)
    def clean(t):
        if not t: return ""
        t = t.lower().strip()
        for m in mfr_set:
            if m in t: t = t.replace(m, " ")
        t = re.sub(r'\b(ref|lot|sn|pn|cat|no|num)[:\s#]*[\w-]+', ' ', t)
        t = re.sub(r'\b\d{5,}\b', ' ', t)
        t = re.sub(r'[^\w\s]', ' ', t)
        return re.sub(r'\s+', ' ', t).strip()
    return F.udf(clean, StringType())

def extract_device_noun(t):
    if not t: return ""
    materials = {'bone', 'metal', 'titanium', 'polyethylene', 'polymer', 'ceramic', 'silicone', 'stainless', 'steel', 'cobalt', 'chrome', 'alloy', 'plastic', 'rubber', 'collagen', 'tissue'}
    generic = {'device', 'system', 'biomedical', 'various', 'other', 'general', 'medical', 'surgical', 'clinical', 'hospital', 'equipment', 'instrument', 'apparatus', 'product', 'kit', 'set'}
    adjectives = {'permanent', 'temporary', 'coated', 'sterile', 'single', 'double', 'reusable', 'disposable', 'implantable', 'external', 'internal', 'left', 'right', 'large', 'small', 'standard', 'primary', 'revision', 'total', 'partial', 'cemented', 'cementless'}
    stopwords = {'with', 'without', 'this', 'that', 'from', 'into', 'using', 'used', 'mm', 'cm', 'ml', 'mg', 'size', 'type', 'model'}
    device_nouns = {'stent', 'valve', 'pacemaker', 'defibrillator', 'catheter', 'graft', 'shunt', 'occluder', 'filter', 'coil', 'lead', 'generator', 'pump', 'balloon', 'prosthesis', 'implant', 'plate', 'screw', 'nail', 'rod', 'pin', 'wire', 'cage', 'spacer', 'cup', 'stem', 'head', 'liner', 'insert', 'anchor', 'fixator', 'staple', 'clip', 'mesh', 'patch', 'sling', 'suture', 'plug', 'lens', 'ring', 'electrode', 'stimulator', 'port', 'drain', 'tube', 'cannula', 'bar'}
    
    all_skip = materials | generic | adjectives | stopwords
    words = [w for w in re.split(r'[,\s/\-]+', t.lower()) if len(w) >= 3 and w.isalpha()]
    for w in words:
        if w in device_nouns: return w
    valid_words = [w for w in words if w not in all_skip]
    return valid_words[-1] if valid_words else ""

extract_device_noun_udf = F.udf(extract_device_noun, StringType())

def sc(df):
    r = df
    for c, t in OUTPUT_SCHEMA.items():
        r = r.withColumn(c, F.lit(None).cast(t)) if c not in df.columns else r.withColumn(c, F.col(c).cast(t))
    return r.select(list(OUTPUT_SCHEMA.keys()))

def check_brand_lookup(cleaned_desc):
    """Check if description matches any brand/term in lookup. Order matters - more specific first."""
    desc_lower = cleaned_desc.lower().strip()
    for brand, (gmdncode, gmdn_name) in BRAND_GMDN_LOOKUP.items():
        if brand in desc_lower:
            return (int(gmdncode), gmdn_name)
    return None

def gmdn_matches_procedure_category(gmdn_name, procedure_keywords):
    if not procedure_keywords or not gmdn_name: return False
    gmdn_lower = gmdn_name.lower()
    return any(kw in gmdn_lower for kw in procedure_keywords)

def get_procedure_categories_for_encounter(encntr_ids):
    """Get OPCS procedure categories for encounters."""
    if not encntr_ids:
        return {}
    
    encntr_list = ",".join([str(e) for e in encntr_ids])
    procs = spark.sql(f"""
        SELECT ENCNTR_ID, Procedure_code 
        FROM {CONFIG['procedures']}
        WHERE ENCNTR_ID IN ({encntr_list})
    """).collect()
    
    result = {}
    for r in procs:
        for prefix, keywords in PROCEDURE_DEVICE_CATEGORIES.items():
            if r.Procedure_code and r.Procedure_code.startswith(prefix):
                if r.ENCNTR_ID not in result:
                    result[r.ENCNTR_ID] = set()
                result[r.ENCNTR_ID].update(keywords)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load/Check Reference Data

# COMMAND ----------

def check_prerequisites():
    missing = []
    for table in ["gmdn_snomed_lookup", "anchor_lookup", "opcs_device_lookup"]:
        try:
            spark.table(CONFIG[table]).limit(1).count()
        except:
            missing.append(table)
    return missing

def load_gmdn_snomed_mapping():
    """Load GMDN->SNOMED mapping from lookup table + manual curated mappings."""
    rows = spark.table(CONFIG["gmdn_snomed_lookup"]).collect()
    mapping = {int(r.gmdncode): (r.snomed_concept_id, r.snomed_concept_name) 
            for r in rows if r.similarity_score and r.similarity_score >= EMBEDDING_SIMILARITY_THRESHOLD}
    # Merge in manual curated mappings (these take priority)
    mapping.update(MANUAL_SNOMED_MAPPINGS)
    return mapping

def enrich_with_snomed(df, mapping):
    if not mapping: return df
    mdf = spark.createDataFrame([(int(k), int(v[0]), str(v[1])) for k, v in mapping.items()], ["gmdncode_lookup", "snomed_id_lookup", "snomed_name_lookup"])
    e = df.join(F.broadcast(mdf), df.gmdncode == mdf.gmdncode_lookup, "left")
    return e.withColumn("snomed_concept_id", F.when(F.col("snomed_concept_id").isNull() & F.col("snomed_id_lookup").isNotNull(), F.col("snomed_id_lookup")).otherwise(F.col("snomed_concept_id"))).withColumn("snomed_name", F.when(F.col("snomed_name").isNull() & F.col("snomed_name_lookup").isNotNull(), F.col("snomed_name_lookup")).otherwise(F.col("snomed_name"))).drop("gmdncode_lookup", "snomed_id_lookup", "snomed_name_lookup")

def assign_confidence_tier(df):
    """Assign confidence tiers based on mapping method and score.
    
    HIGH:   Layers 1-3 (GS1, exact anchor, cleaned anchor) + Layer 5 brand matches (conf >= 0.90)
    MEDIUM: Layer 4 (OPCS->SNOMED) + Layer 5 embedding matches (conf >= 0.65)
    LOW:    Layer 5 procedure-boosted matches (conf < 0.65)
    """
    return df.withColumn("confidence_tier",
        F.when(F.col("mapping_layer").isin(
            "LAYER1_GS1_GUDID", 
            "LAYER2_EXACT_ANCHOR", "LAYER2_ANCHOR_EXACT",
            "LAYER3_CLEANED_ANCHOR", "LAYER3_ANCHOR_CLEANED"
        ), F.lit("HIGH"))
         .when((F.col("mapping_layer") == "LAYER5_GMDN_EMBEDDING") & (F.col("mapping_confidence") >= 0.90), F.lit("HIGH"))
         .when(F.col("mapping_layer") == "LAYER4_OPCS_SNOMED", F.lit("MEDIUM"))
         .when((F.col("mapping_layer") == "LAYER5_GMDN_EMBEDDING") & (F.col("mapping_confidence") >= DEFAULT_LAYER5_THRESHOLD), F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )

def load_existing_layer5_matches():
    try:
        df = spark.table(CONFIG["layer5_matches"])
        rows = df.collect()
        # Cast gmdncode to int when loading from cache
        return {r.cleaned_desc: {
            'gmdncode': int(r.gmdncode) if r.gmdncode else None,
            'gmdn_name': r.gmdn_name,
            'similarity_score': r.similarity_score,
            'match_status': r.match_status
        } for r in rows}
    except:
        return {}

def invalidate_stale_cache(existing_matches):
    """Remove stale cache entries that should be re-evaluated.
    
    Invalidates entries where:
    1. A brand term now matches (brand lookup should take priority over embedding match)
    2. The cached GMDN match is now excluded by FP prevention rules
    """
    stale_descs = set()
    
    for desc, match_info in list(existing_matches.items()):
        # If brand lookup would catch this description, invalidate the cache entry
        if match_info.get('match_status') != 'brand_match' and check_brand_lookup(desc) is not None:
            stale_descs.add(desc)
            continue
        
        # If a cached 'matched' or 'matched_boosted' entry now fails FP validation,
        # invalidate so it gets re-evaluated against top-10 alternatives
        if match_info.get('match_status') in ('matched', 'matched_boosted') and match_info.get('gmdn_name'):
            if not is_valid_match(desc, match_info['gmdn_name']):
                stale_descs.add(desc)
    
    for desc in stale_descs:
        del existing_matches[desc]
    
    return len(stale_descs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Pipeline

# COMMAND ----------

def run_incremental():
    print("="*60 + "\nDEVICE MAPPING PIPELINE (INCREMENTAL - Full GMDN Comparison)\n" + "="*60)
    
    missing = check_prerequisites()
    if missing:
        print(f"\n[ERROR] Missing reference tables: {missing}")
        print("Run the full Device_Mapping_Pipeline first.")
        return None
    
    print("\n[1] Finding new records...")
    try:
        existing_ids = spark.table(CONFIG["device_mapping_output"]).select("EVENT_ID")
        all_records = spark.table(CONFIG["implant_details"]).select("EVENT_ID", "ENCNTR_ID", "PERSON_ID", "IMPLANT_DESCRIPTION")
        new_records = all_records.join(existing_ids, "EVENT_ID", "left_anti")
    except:
        new_records = spark.table(CONFIG["implant_details"]).select("EVENT_ID", "ENCNTR_ID", "PERSON_ID", "IMPLANT_DESCRIPTION")
    
    new_records.createOrReplaceTempView("new_records")
    new_count = spark.sql("SELECT COUNT(*) as cnt FROM new_records").collect()[0].cnt
    print(f"    {new_count:,} new records")
    
    if new_count == 0:
        print("\n    No new records to process. Done.")
        return None
    
    print("\n[2] Loading reference data...")
    gm_mapping = load_gmdn_snomed_mapping()
    print(f"    GMDN->SNOMED: {len(gm_mapping)}")
    
    anchors = spark.table(CONFIG["anchor_lookup"])
    anchors.createOrReplaceTempView("anchors")
    print(f"    Anchors: {anchors.count():,}")
    
    opcs = spark.table(CONFIG["opcs_device_lookup"])
    opcs.createOrReplaceTempView("opcs_lookup")
    print(f"    OPCS: {opcs.count():,}")
    
    print("\n[2b] Loading GMDN embedding matrix...")
    load_gmdn_embedding_matrix()
    
    existing_l5_matches = load_existing_layer5_matches()
    print(f"    Cached L5 matches: {len(existing_l5_matches):,}")
    
    # Invalidate stale cache entries (brand overrides + FP exclusions)
    stale_count = invalidate_stale_cache(existing_l5_matches)
    if stale_count > 0:
        print(f"    Invalidated {stale_count:,} stale cache entries (brand overrides + FP exclusions)")
    
    mfrs = get_manufacturer_names()
    clean_udf = create_text_cleaning_udf(mfrs)
    
    # Layer 1: GS1->GUDID
    print("\n[3] Layer 1: GS1->GUDID...")
    l1 = spark.sql("""
        WITH gs1 AS (SELECT EVENT_ID, ENCNTR_ID, PERSON_ID, IMPLANT_DESCRIPTION, GS1_IDENTIFIER 
                     FROM {implant_details} WHERE GS1_IDENTIFIER IS NOT NULL AND LENGTH(GS1_IDENTIFIER) >= 10),
        gm AS (SELECT i.*, g.gmdnptdefinition as gmdn_name, CAST(g.gmdncode AS BIGINT) as gmdncode
               FROM gs1 i JOIN (SELECT d.primarydi, g.gmdnptdefinition, g.gmdncode, ROW_NUMBER() OVER (PARTITION BY d.primarydi ORDER BY g.gmdncode) as rn
                                FROM {gudid_device} d JOIN {gudid_gmdnterms} g ON d.primarydi = g.primarydi WHERE g.gmdncode IS NOT NULL) g 
               ON i.GS1_IDENTIFIER = g.primarydi AND g.rn = 1)
        SELECT EVENT_ID, ENCNTR_ID, PERSON_ID, IMPLANT_DESCRIPTION, gmdncode, gmdn_name, 
               CAST(NULL AS BIGINT) as snomed_concept_id, CAST(NULL AS STRING) as snomed_name, 
               'LAYER1_GS1_GUDID' as mapping_layer, 1.0 as mapping_confidence FROM gm
    """.format(**CONFIG)).join(spark.table("new_records").select("EVENT_ID"), "EVENT_ID", "inner").withColumn("device_type", extract_device_noun_udf(F.col("gmdn_name")))
    l1.createOrReplaceTempView("layer1_results")
    l1_count = spark.sql("SELECT COUNT(*) as cnt FROM layer1_results").collect()[0].cnt
    print(f"    {l1_count:,}")
    
    # Update anchors
    new_anchors = spark.sql("""
        SELECT LOWER(TRIM(IMPLANT_DESCRIPTION)) as normalized_desc,
               FIRST(gmdncode) as gmdncode, FIRST(gmdn_name) as gmdn_name, 
               FIRST(device_type) as device_type, COUNT(*) as anchor_count
        FROM layer1_results GROUP BY LOWER(TRIM(IMPLANT_DESCRIPTION))
    """)
    combined_anchors = spark.table("anchors").unionByName(new_anchors).groupBy("normalized_desc").agg(
        F.first("gmdncode").alias("gmdncode"), F.first("gmdn_name").alias("gmdn_name"),
        F.first("device_type").alias("device_type"), F.sum("anchor_count").alias("anchor_count"))
    combined_anchors.createOrReplaceTempView("combined_anchors")
    
    spark.sql("CREATE OR REPLACE TEMP VIEW excl_l2 AS SELECT EVENT_ID FROM layer1_results")
    
    # Layer 2: Exact Anchor
    print("[4] Layer 2: Exact Anchor...")
    l2 = spark.table("new_records").join(spark.table("excl_l2"), "EVENT_ID", "left_anti") \
        .withColumn("normalized_desc", F.lower(F.trim(F.col("IMPLANT_DESCRIPTION")))) \
        .join(spark.table("combined_anchors"), "normalized_desc", "inner") \
        .withColumn("snomed_concept_id", F.lit(None).cast(LongType())) \
        .withColumn("snomed_name", F.lit(None).cast(StringType())) \
        .withColumn("mapping_layer", F.lit("LAYER2_ANCHOR_EXACT")) \
        .withColumn("mapping_confidence", F.lit(0.95).cast(DoubleType()))
    l2.createOrReplaceTempView("layer2_results")
    l2_count = spark.sql("SELECT COUNT(*) as cnt FROM layer2_results").collect()[0].cnt
    print(f"    {l2_count:,}")
    
    spark.sql("CREATE OR REPLACE TEMP VIEW excl_l3 AS SELECT EVENT_ID FROM layer1_results UNION ALL SELECT EVENT_ID FROM layer2_results")
    
    # Layer 3: Cleaned Anchor
    print("[5] Layer 3: Cleaned Anchor...")
    cleaned_anchors = spark.table("combined_anchors").withColumn("cleaned_desc", F.col("normalized_desc"))
    cleaned_anchors.createOrReplaceTempView("cleaned_anchors")
    
    l3_input = spark.table("new_records").join(spark.table("excl_l3"), "EVENT_ID", "left_anti") \
        .withColumn("cleaned_desc", clean_udf(F.col("IMPLANT_DESCRIPTION"))) \
        .filter(F.length(F.col("cleaned_desc")) >= 5)
    l3 = l3_input.join(spark.table("cleaned_anchors").select("cleaned_desc", "gmdncode", "gmdn_name", "device_type"), "cleaned_desc", "inner") \
        .withColumn("snomed_concept_id", F.lit(None).cast(LongType())) \
        .withColumn("snomed_name", F.lit(None).cast(StringType())) \
        .withColumn("mapping_layer", F.lit("LAYER3_ANCHOR_CLEANED")) \
        .withColumn("mapping_confidence", F.lit(0.85).cast(DoubleType()))
    l3.createOrReplaceTempView("layer3_results")
    l3_count = spark.sql("SELECT COUNT(*) as cnt FROM layer3_results").collect()[0].cnt
    print(f"    {l3_count:,}")
    
    spark.sql("CREATE OR REPLACE TEMP VIEW excl_l4 AS SELECT EVENT_ID FROM layer1_results UNION ALL SELECT EVENT_ID FROM layer2_results UNION ALL SELECT EVENT_ID FROM layer3_results")
    
    # Layer 4: OPCS->SNOMED
    print("[6] Layer 4: OPCS->SNOMED...")
    l4_input = spark.sql("SELECT DISTINCT mid.EVENT_ID, mid.ENCNTR_ID, mid.PERSON_ID, mid.IMPLANT_DESCRIPTION, p.Procedure_code as opcs_code FROM {implant_details} mid JOIN {procedures} p ON mid.ENCNTR_ID = p.ENCNTR_ID WHERE p.Catalogue = 'OPCS4'".format(**CONFIG))
    l4_input = l4_input.join(spark.table("new_records").select("EVENT_ID"), "EVENT_ID", "inner").join(spark.table("excl_l4"), "EVENT_ID", "left_anti")
    l4_joined = l4_input.join(spark.table("opcs_lookup").select("opcs_code", "device_concept_id", "device_name", "device_type"), "opcs_code", "inner")
    w4 = Window.partitionBy("EVENT_ID").orderBy(F.col("device_concept_id"))
    l4 = l4_joined.withColumn("rank", F.row_number().over(w4)).filter(F.col("rank") == 1) \
        .withColumn("gmdncode", F.lit(None).cast(LongType())) \
        .withColumn("gmdn_name", F.lit(None).cast(StringType())) \
        .withColumnRenamed("device_concept_id", "snomed_concept_id") \
        .withColumnRenamed("device_name", "snomed_name") \
        .withColumn("mapping_layer", F.lit("LAYER4_OPCS_SNOMED")) \
        .withColumn("mapping_confidence", F.lit(0.75).cast(DoubleType())) \
        .drop("rank", "opcs_code")
    l4.createOrReplaceTempView("layer4_results")
    l4_count = spark.sql("SELECT COUNT(*) as cnt FROM layer4_results").collect()[0].cnt
    print(f"    {l4_count:,}")
    
    spark.sql("CREATE OR REPLACE TEMP VIEW excl_l5 AS SELECT EVENT_ID FROM layer1_results UNION ALL SELECT EVENT_ID FROM layer2_results UNION ALL SELECT EVENT_ID FROM layer3_results UNION ALL SELECT EVENT_ID FROM layer4_results")
    
    # Layer 5: GMDN Embedding (FULL comparison with FP validation)
    print("[7] Layer 5: GMDN Embedding (FULL comparison with FP validation)...")
    l5_input = spark.table("new_records").join(spark.table("excl_l5"), "EVENT_ID", "left_anti")
    l5_unique = l5_input.withColumn("cleaned_desc", clean_udf(F.col("IMPLANT_DESCRIPTION"))) \
        .filter(F.length(F.col("cleaned_desc")) >= 3) \
        .groupBy("cleaned_desc").agg(F.count("*").alias("cnt"), F.collect_set("ENCNTR_ID").alias("encntr_ids")) \
        .orderBy(F.desc("cnt"))
    unique_list = [(r.cleaned_desc, r.cnt, list(r.encntr_ids)[:50]) for r in l5_unique.collect()]
    
    # Filter already processed
    to_process = [(d, c, e) for d, c, e in unique_list if d not in existing_l5_matches]
    from_cache = [(d, c) for d, c, e in unique_list if d in existing_l5_matches]
    print(f"    {len(unique_list)} unique, {len(from_cache)} cached, {len(to_process)} to process")
    
    l5_results = []
    l5_cache_updates = []  # Track all results for cache update
    brand_matches = 0
    boosted_matches = 0
    rejected_fp = 0
    
    for idx, (desc, cnt, encntr_ids) in enumerate(to_process):
        # Brand lookup first - check_brand_lookup now returns int(gmdncode)
        brand_match = check_brand_lookup(desc)
        if brand_match:
            gmdncode, gmdn_name = brand_match
            l5_results.append({'cleaned_desc': desc, 'gmdncode': gmdncode, 'gmdn_name': gmdn_name, 'similarity_score': 0.95})
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': gmdncode, 'gmdn_name': gmdn_name, 'similarity_score': 0.95, 'match_status': 'brand_match'})
            brand_matches += 1
            continue
        
        # Get embedding for description
        embs = get_embeddings_batch_cached([desc], text_type='layer5', batch_size=1)
        desc_emb = embs[0]
        if desc_emb is None:
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': None, 'gmdn_name': None, 'similarity_score': 0.0, 'match_status': 'no_embedding'})
            continue
        
        # Find best match using FULL matrix comparison with FP validation
        best_code, best_name, best_sim, was_rejected = find_best_gmdn_match(desc_emb, desc)
        
        if best_code is None:
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': None, 'gmdn_name': None, 'similarity_score': 0.0, 'match_status': 'no_candidates'})
            continue
        
        # If all top 10 were rejected as false positives
        if was_rejected:
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': best_code, 'gmdn_name': best_name, 'similarity_score': best_sim, 'match_status': 'rejected_fp'})
            rejected_fp += 1
            continue
        
        # Get procedure categories for threshold boosting
        proc_categories = get_procedure_categories_for_encounter(encntr_ids)
        procedure_keywords = set()
        for enc_keywords in proc_categories.values():
            procedure_keywords.update(enc_keywords)
        
        # Determine threshold
        threshold = DEFAULT_LAYER5_THRESHOLD
        is_boosted = False
        if procedure_keywords and best_name and gmdn_matches_procedure_category(best_name, procedure_keywords):
            threshold = PROCEDURE_BOOSTED_THRESHOLD
            is_boosted = True
        
        if best_sim >= threshold:
            l5_results.append({'cleaned_desc': desc, 'gmdncode': int(best_code), 'gmdn_name': best_name, 'similarity_score': best_sim})
            match_status = 'matched_boosted' if is_boosted else 'matched'
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': best_code, 'gmdn_name': best_name, 'similarity_score': best_sim, 'match_status': match_status})
            if is_boosted:
                boosted_matches += 1
        else:
            l5_cache_updates.append({'cleaned_desc': desc, 'gmdncode': best_code, 'gmdn_name': best_name, 'similarity_score': best_sim, 'match_status': 'below_threshold'})
        
        if (idx+1) % 200 == 0: 
            print(f"    Processed {idx+1}/{len(to_process)} (brand: {brand_matches}, boosted: {boosted_matches}, rejected_fp: {rejected_fp})")
            save_new_embeddings()
    
    save_new_embeddings()
    
    # Save cache updates
    if l5_cache_updates:
        cache_schema = StructType([
            StructField("cleaned_desc", StringType()),
            StructField("gmdncode", LongType()),
            StructField("gmdn_name", StringType()),
            StructField("similarity_score", DoubleType()),
            StructField("match_status", StringType())
        ])
        cache_df = spark.createDataFrame(l5_cache_updates, schema=cache_schema) \
            .withColumn("processed_at", F.current_timestamp())
        cache_df.write.mode("append").saveAsTable(CONFIG["layer5_matches"])
        print(f"    Saved {len(l5_cache_updates)} cache entries")
    
    # Add cached results - gmdncode is already cast to int in load_existing_layer5_matches
    for desc, cnt in from_cache:
        cached = existing_l5_matches[desc]
        if cached['match_status'] in ('matched', 'matched_boosted', 'brand_match') and cached['gmdncode']:
            l5_results.append({
                'cleaned_desc': desc, 'gmdncode': int(cached['gmdncode']),
                'gmdn_name': cached['gmdn_name'], 'similarity_score': cached['similarity_score']
            })
    
    print(f"    {len(l5_results)} matches (brand: {brand_matches}, boosted: {boosted_matches}, rejected_fp: {rejected_fp})")
    
    if l5_results:
        l5_match_df = spark.createDataFrame(l5_results, schema=StructType([
            StructField("cleaned_desc", StringType()), StructField("gmdncode", LongType()),
            StructField("gmdn_name", StringType()), StructField("similarity_score", DoubleType())
        ]))
        l5 = l5_input.withColumn("cleaned_desc", clean_udf(F.col("IMPLANT_DESCRIPTION"))) \
            .join(F.broadcast(l5_match_df), "cleaned_desc", "inner") \
            .withColumn("snomed_concept_id", F.lit(None).cast(LongType())) \
            .withColumn("snomed_name", F.lit(None).cast(StringType())) \
            .withColumn("device_type", extract_device_noun_udf(F.col("gmdn_name"))) \
            .withColumn("mapping_layer", F.lit("LAYER5_GMDN_EMBEDDING")) \
            .withColumn("mapping_confidence", F.col("similarity_score").cast(DoubleType())) \
            .drop("cleaned_desc", "similarity_score")
    else:
        l5 = spark.createDataFrame([], schema=StructType([StructField(c, t) for c, t in OUTPUT_SCHEMA.items()]))
    l5.createOrReplaceTempView("layer5_results")
    l5_count = spark.sql("SELECT COUNT(*) as cnt FROM layer5_results").collect()[0].cnt
    print(f"    {l5_count:,} records")
    
    # Combine and enrich
    print("\n[8] Combining results...")
    all_new = sc(spark.table("layer1_results")).unionByName(sc(spark.table("layer2_results"))) \
        .unionByName(sc(spark.table("layer3_results"))).unionByName(sc(spark.table("layer4_results"))) \
        .unionByName(sc(spark.table("layer5_results")))
    
    print("[9] Enriching with SNOMED...")
    all_new = enrich_with_snomed(all_new, gm_mapping)
    
    print("[9b] Assigning confidence tiers...")
    all_new = assign_confidence_tier(all_new)
    
    print("[9c] Including unmapped records...")
    mapped_ids = all_new.select("EVENT_ID")
    unmapped = (spark.table("new_records").join(mapped_ids, "EVENT_ID", "left_anti")
        .withColumn("gmdncode", F.lit(None).cast(LongType()))
        .withColumn("gmdn_name", F.lit(None).cast(StringType()))
        .withColumn("snomed_concept_id", F.lit(None).cast(LongType()))
        .withColumn("snomed_name", F.lit(None).cast(StringType()))
        .withColumn("device_type", F.lit(None).cast(StringType()))
        .withColumn("mapping_layer", F.lit("UNMAPPED"))
        .withColumn("mapping_confidence", F.lit(0.0).cast(DoubleType()))
        .withColumn("confidence_tier", F.lit(None).cast(StringType()))
    )
    unmapped_count = unmapped.count()
    print(f"    {unmapped_count:,} unmapped records included")
    all_new = all_new.unionByName(unmapped)
    
    total_mapped = all_new.filter(F.col("mapping_layer") != "UNMAPPED").count()
    with_snomed = all_new.filter(F.col("snomed_concept_id").isNotNull()).count()
    
    print(f"\n{'='*60}")
    print(f"New records: {new_count:,}")
    print(f"Mapped: {total_mapped:,} ({100.0*total_mapped/new_count:.1f}%)")
    print(f"With SNOMED: {with_snomed:,}")
    
    all_new.createOrReplaceTempView("new_mappings")
    print("\nConfidence tier breakdown:")
    spark.sql("""
        SELECT confidence_tier, COUNT(*) as count,
               SUM(CASE WHEN snomed_concept_id IS NOT NULL THEN 1 ELSE 0 END) as with_snomed
        FROM new_mappings GROUP BY confidence_tier ORDER BY confidence_tier
    """).show(truncate=False)
    
    print("\n[10] Saving...")
    spark.sql(f"""
        MERGE INTO {CONFIG['device_mapping_output']} t
        USING new_mappings s ON t.EVENT_ID = s.EVENT_ID
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    spark.table("combined_anchors").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CONFIG["anchor_lookup"])
    
    final_count = spark.table(CONFIG["device_mapping_output"]).count()
    print(f"    Output: {final_count:,} total")
    
    return all_new

# COMMAND ----------

results = run_incremental()
