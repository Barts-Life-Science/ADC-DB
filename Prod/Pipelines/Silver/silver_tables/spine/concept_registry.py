"""Silver spine.concept_registry: 1 dataset(s).

- datasets: reference_concept_registry
- moved verbatim from: 1_silver_spine_and_reference:5290, 1_silver_spine_and_reference:5293

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, canonical_coding_system, read_source, stable_id
from silver_tables.spine._shared import _lifecycle_source_concept_map


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# INSR R1: coding systems whose lane source codes the registry carries (today's registry, read-only query of R1 Task 17).
R1_REGISTRY_SOURCE_SYSTEMS = ('urn:cerner:encounter:reason-for-visit', 'urn:cerner:clinical-event:presenting-complaint-text', 'urn:cerner:surginet:procedure', 'http://snomed.info/sct', 'urn:barts:pathology:gene-symbol', 'urn:cerner:code_value', 'urn:cerner:nomenclature', 'urn:sectra:examination-code', 'http://hl7.org/fhir/sid/icd-10', 'http://fhir.hl7.org.uk/CodeSystem/OPCS-4', 'urn:endobase:dgvs-term', 'urn:cerner:synonym_id', 'urn:jac:drug-id', 'urn:cerner:scheduling:appointment-type', 'urn:cerner:nomenclature:tfc', 'urn:barts:jac:drug-id', 'urn:barts:slam:hrg', 'urn:cerner:radiology-exam', 'urn:barts:pathology:report-code', 'urn:cerner:order_synonym_id', 'urn:sectra:imaging-report', 'urn:barts:pathology:specimen-type', 'urn:barts:implant:primary-procedure', 'urn:cerner:order-mnemonic', 'urn:cerner:nomenclature:cerner-testcode', 'urn:cerner:nomenclature:uk-ed-subset', 'urn:cerner:mill:planned-procedure', 'urn:barts:sact:drug-token', 'urn:barts:registry-field', 'urn:barts:winpath:lims2:organism-code', 'urn:cerner:form_ref_id', 'urn:barts:winpath:lims5:organism-code', 'urn:cerner:event_cd', 'urn:cerner:nomenclature:multum-drug-identifier', 'urn:barts:hcd:chargeable-item', 'urn:jac:homecare:item', 'urn:cerner:dcp_input_ref_id', 'urn:barts:luna:treatment-function', 'urn:cerner:nomenclature:patient-care', 'urn:cerner:nomenclature:ed-presenting-complaint', 'urn:barts:winpath:lims2:agent-code', 'urn:barts:pathology:unit', 'urn:barts:winpath:lims5:agent-code', 'urn:isbt:product-code', 'urn:nhs:ccmds:activity', 'urn:barts:endobase:exam-type', 'urn:cerner:nomenclature:winpath-lims2', 'urn:iweb:reg_cs2010g_post1:culturetxt', 'urn:cerner:nomenclature:winpath-lims5', 'urn:cerner:nomenclature:multum-allergy-category', 'urn:iweb:reg_mort:cardiologyattendeestxt', 'urn:iweb:reg_mdt:cardiologyattendeestxt', 'urn:dicom:sop-class-uid', 'urn:iweb:reg_noncoronary:interventionstxt', 'urn:omop:concept_id', 'urn:nhs:nccmds:activity', 'urn:iweb:reg_eracs:postprotamineact', 'urn:iweb:reg_cs2010g_pre1:ctaortaextentofdissectiontxt', 'urn:iweb:reg_cs2010g_pre1:otheractualcardiacprocedurestxt', 'urn:iweb:reg_ctmdt:cardiothoracicattendeestxt', 'urn:iweb:reg_coronary_subprocedure:drugselutedbystentstxt', 'urn:iweb:reg_mdt:cardiothoracicattendeestxt', 'urn:barts:luna:rtt-status', 'urn:iweb:reg_coronary_subprocedure:proceduraldevicesusedtxt', 'urn:iweb:reg_mort:significantcoexistingfactorstxt', 'urn:iweb:reg_cs2010g_pre2:aorticproceduretxt', 'urn:cerner:coded-event:event_result_txt', 'urn:iweb:reg_mort:specialitytxt', 'urn:iweb:reg_cs2010g_followup:postoperativecomplicationtxt', 'urn:iweb:reg_cs2010g_pre1:aetiologytxt', 'urn:iweb:reg_cs2010g_pre1:mpreoperationhistory', 'urn:iweb:reg_cs2010g_post1:cabgcodingtxt', 'urn:iweb:reg_cs2010g_pre2:aorticimplantlocationtxt', 'urn:iweb:reg_cs2010g_subprocedure:valveorringmodelnumber', 'urn:barts:mediconnect:device-type', 'urn:iweb:reg_dghminap:ecgsubcategoriestxt', 'urn:iweb:reg_dghminap:otherdischargediagnosistxt', 'urn:cerner:nomenclature:allergy', 'urn:iweb:reg_cs2010g_post2:postoperativecomplicationtxt', 'urn:iweb:reg_eracs:procedureperformedtxt', 'urn:iweb:reg_eracs:dripstxt', 'urn:iweb:reg_noncoronary:investigationstxt', 'urn:medicus:critical-care-score', 'urn:iweb:reg_noncoronary:arterialcomplicationstxt', 'urn:iweb:reg_cs2010g_pre1:mechanismofmitralregurgitationtxt', 'urn:iweb:reg_mdt:cardiacriskfactorstxt', 'urn:nhs:msds:delivery-method', 'urn:iweb:reg_ctmdt:cardiacriskfactorstxt', 'urn:iweb:reg_cs2010g_subprocedure:aorticpathologytxt', 'urn:iweb:reg_cs2010g_post1:procedureperformedtxt', 'urn:cerner:nomenclature:hrg', 'urn:barts:pathology:wkg-tlc', 'urn:iweb:reg_cs2010g_post1:valvesreplacementcodingtxt', 'urn:barts:slam:pod', 'urn:iweb:reg_cs2010g_pre2:arterialcannulationnicortxt', 'urn:barts:bloodtrack:blood-group', 'urn:iweb:reg_noncoronary:proceduralcomplicationtxt', 'urn:iweb:reg_coronary_subprocedure:diagnosticdevicesusedduringproceduretxt', 'urn:iweb:reg_cs2010g_subprocedure:reasonforrepeatvalvereplacementtxt', 'urn:iweb:reg_cs2010g_pre2:aorticpathologytxt', 'urn:bloodtrack:blood-group', 'urn:iweb:reg_ctmdt:aetiologytxt', 'urn:iweb:reg_cs2010g_pre1:previouscardiacsurgerytxt', 'urn:cerner:order-comment-type', 'urn:cerner:coded-event:event_cd', 'urn:iweb:reg_cs2010g_pre1:presentationtxt', 'urn:iweb:reg_cs2010g_post1:aorticrootandreplacementcodingtxt', 'urn:iweb:reg_cs2010g_post2:ssisitetxt', 'urn:iweb:reg_ctmdt:relevantmedicationtxt', 'urn:iweb:reg_cs2010g_post1:stagestxt', 'urn:iweb:reg_eracs:bloodproductsadministeredtxt', 'urn:iweb:reg_cs2010g_post1:returntotheatretxt', 'urn:iweb:reg_mdt:anginalmedicationtxt', 'urn:cerner:blob-content-type', 'urn:iweb:reg_cs2010g_pre1:surgicalincisiontxt', 'urn:iweb:reg_coronary_subprocedure:atherothrombusremovaldevicesusedtxt', 'urn:iweb:reg_cs2010g_pre1:clinicalpresentationmalperfusiontxt', 'urn:datix:incident-type', 'urn:iweb:reg_cs2010g_post1:mitralvalvecodingtxt', 'urn:iweb:reg_cs2010g_post1:postop_caretxt', 'urn:iweb:reg_cs2010g_post1:otheroperationsonventriclesofthehearttxt', 'urn:iweb:reg_cs2010g_subprocedure:svcharacteristicstxt', 'urn:iweb:reg_cs2010g_post1:commoncodingtxt', 'urn:iweb:reg_eracs:medicalhistorytxt', 'urn:iweb:reg_cs2010g_post1:bacteriatxt', 'urn:badgernet:narrative-field', 'urn:iweb:reg_eracs:underlyingrhythmtxt', 'urn:iweb:reg_cs2010g_post1:neuroprotectiontxt', 'urn:iweb:reg_dghminap:stressprecipitanttxt', 'urn:iweb:reg_eracs:postopanalgesiatxt', 'urn:iweb:reg_cs2010g_pre1:clinicalpresentationtxt', 'urn:iweb:reg_cs2010g_post1:aorticvalvecodingtxt', 'urn:iweb:reg_coronary_subprocedure:stenosisprepci', 'urn:barts:community:activity-type', 'urn:iweb:reg_cs2010g_pre2:aorticsegmentsoperatedontxt', 'urn:iweb:reg_cs2010g_post1:drain_sitetxt', 'urn:iweb:reg_cs2010g_pre1:historyofneurologicaldiseasetxt', 'urn:iweb:reg_cs2010g_post1:pacingwirestxt', 'urn:iweb:reg_cs2010g_post1:ventricularassistdeviceusedtxt', 'urn:iweb:reg_cs2010g_post1:avsdcodingtxt', 'urn:iweb:reg_cs2010g_post1:intraaorticballoonpumpusedtxt', 'urn:nhs:msds:labour-onset-method', 'urn:iweb:reg_cs2010g_followup:ixsatisfactorypriortodischargetxt', 'urn:barts:bloodtrack:product-group', 'urn:iweb:reg_mdt:causeofdeviationtxt', 'urn:iweb:reg_ctmdt:rationaleforcontradictingtxt', 'urn:barts:pathology:assay-code', 'urn:iweb:reg_cs2010g_post1:asdcodingtxt', 'urn:cerner:research-status', 'urn:iweb:reg_coronary_subprocedure:stenosispostpci', 'urn:bloodtrack:product-group', 'urn:iweb:reg_eracs:complicationstxt', 'urn:iweb:reg_eracs:veinharvestsitetxt', 'urn:iweb:reg_eracs:chestdrainstxt', 'urn:iweb:reg_cs2010g_subprocedure:maorticgraftdetail', 'urn:iweb:reg_ctmdt:cardiologyattendeestxt', 'urn:iweb:reg_cs2010g_subprocedure:harvestingcharacteristicstxt', 'urn:iweb:reg_cs2010g_pre1:ctaortadiagnosistxt', 'urn:iweb:reg_cs2010g_post1:mclosuredetails', 'urn:iweb:reg_cs2010g_post1:othersupportdeviceusedtxt', 'urn:iweb:reg_noncoronary:mdetailsofalldevicesimplanted', 'urn:iweb:reg_cs2010g_post1:newpost_operativeneurologicaldysfunctiontxt', 'urn:iweb:reg_mdt:epattendeestxt', 'urn:iweb:reg_cs2010g_pre1:ctaortaprimaryentryteartxt', 'urn:iweb:reg_noncoronary:mindicationsandbackgroundhistory', 'urn:iweb:reg_cs2010g_pre1:previousendovascularsurgerytxt', 'urn:iweb:reg_mdt:treatmentdeviationtxt', 'urn:iweb:reg_mdt:viabilityassessmenttxt', 'urn:iweb:reg_cs2010g_pre2:previouspercutaneousvalveimplantsitetxt', 'urn:iweb:reg_mdt:significantvalvediseasetxt', 'urn:iweb:reg_cs2010g_post1:sternalwiretxt', 'urn:iweb:reg_coronary_subprocedure:guidecathetertxt', 'urn:iweb:reg_cs2010g_post2:lrtitxt', 'urn:cerner:nomenclature:icnarccmp', 'urn:iweb:reg_ctmdt:viabilityassessmenttxt', 'urn:iweb:reg_cs2010g_followup:lrtitxt', 'urn:iweb:reg_ctmdt:significantvalvediseasetxt', 'urn:iweb:reg_cs2010g_post1:pericardialdraintxt', 'urn:iweb:reg_mdt:significantnoncoronarycardiachistorytxt', 'urn:iweb:reg_dghminap:positiveinotropicsupporttxt', 'urn:iweb:reg_cs2010g_pre1:myectomyoptionstxt', 'urn:iweb:reg_cs2010g_post1:leftpleuraldraintxt', 'urn:iweb:reg_cs2010g_pre2:venouscannulationnicortxt', 'urn:iweb:reg_cs2010g_post1:mediastinaldraintxt', 'urn:iweb:reg_cs2010g_pre1:preoperativetranscatheterinterventionstxt', 'urn:iweb:reg_cs2010g_post1:psvandpapvdcodingtxt', 'urn:iweb:reg_cs2010g_post1:rightpleuraldraintxt', 'urn:iweb:reg_cs2010g_post1:cardioplegia_temperaturetxt', 'urn:iweb:reg_cs2010g_post1:vsdcodingtxt', 'urn:iweb:reg_cs2010g_post1:tricuspidvalvecodingtxt', 'urn:iweb:reg_cs2010g_post1:veinharvestsitetxt', 'urn:iweb:reg_noncoronary:mdischargemanagementplan', 'urn:iweb:reg_eracs:antiplateletstxt', 'urn:iweb:reg_cs2010g_followup:postoperativepyrexiatreatmenttxt', 'urn:barts:community:consultation-type', 'urn:cerner:nomenclature:general-lab', 'urn:iweb:reg_eracs:postopplantxt', 'urn:iweb:reg_cs2010g_post1:woundsubcutaneoustissuedraintxt', 'urn:iweb:reg_mdt:mcmrdetail', 'urn:iweb:reg_cs2010g_pre1:pre_opsupportdevicesusedtxt', 'urn:iweb:reg_cs2010g_pre1:surgicalincisionnonnicoroptionstxt', 'urn:iweb:reg_cs2010g_pre1:mitralvalvediseasetxt', 'urn:iweb:reg_coronary_subprocedure:emboliprotectiondevicesusedtxt', 'urn:iweb:reg_cs2010g_post1:cardioplegia_infusionmodetxt', 'urn:iweb:reg_ctmdt:otherconsultantsname', 'urn:iweb:reg_eracs:antibioticstxt', 'urn:iweb:reg_mort:surgicalattendeestxt', 'urn:iweb:reg_ctmdt:significantnoncoronarycardiachistorytxt', 'urn:iweb:reg_cs2010g_pre1:aorticvalvediseasetxt', 'urn:iweb:reg_cs2010g_pre1:previousabdominaldescendingaorticsurgtxt', 'urn:iweb:reg_eracs:vcapthreshold', 'urn:iweb:reg_cs2010g_followup:sternalwoundinfectiontreatmenttxt', 'urn:iweb:reg_cs2010g_post1:mtoefindings', 'urn:iweb:reg_cs2010g_followup:harvestwoundinfectiontreatmenttxt', 'urn:iweb:reg_cs2010g_post1:pulmonaryvalvecodingtxt', 'urn:cerner:nomenclature:anatomic-pathology', 'urn:luna:eal-comment', 'urn:barts:pathology:alteration-type', 'urn:cerner:nomenclature:microbiology', 'urn:iweb:reg_noncoronary:mprocedurefindingsandreport', 'urn:badgernet:examination', 'urn:iweb:reg_coronary_subprocedure:guideextensiontxt', 'urn:iweb:reg_eracs:ulcerprophylaxistxt', 'urn:iweb:reg_mdt:mpresentinghistory', 'urn:iweb:reg_eracs:allergiestxt', 'urn:iweb:reg_cs2010g_post1:dorvcodingtxt', 'urn:iweb:reg_ctmdt:mdiscussion', 'urn:cerner:nomenclature:cerner', 'urn:cerner:nomenclature:icd-10-am', 'urn:iweb:reg_cs2010g_post1:impellerdeviceusedtxt', 'urn:endobase:document-type', 'urn:iweb:reg_cs2010g_post1:atrialappendagecodingtxt', 'urn:iweb:reg_eracs:antiemetictxt', 'urn:iweb:reg_cs2010g_post2:sendtonicortxt',)


@materialized_view(
    name=_n("journey_reference.concept_registry"),
    refresh_policy="auto",
)
def concept_registry():
    # Build the declared dataset: Observed source and target coding-system/code pairs across the bronze mapping
    # lanes, the native arms and the concept map.
    # INSR R1: rebased from the 55 event feeds onto bronze and 3_lookup (an S1 pass-through with no feed edges).
    # Counts are now bronze source-grain rows per (lane, map_key) from mapping_resolution_usage, not silver feed
    # rows after component split, dedup and scope; totals differ from the feed-based registry, keys do not.
    # Native arms (method 'native') carry the coded feed columns of the models no lane maps. Lane source codes are
    # kept only in the coding systems today's registry carries (R1_REGISTRY_SOURCE_SYSTEMS, read from prod in R1
    # Task 17), so the free-text keys of the result/value lanes (tens of millions) do not enter the registry. The
    # pathology result-value systems are left out of that list: today's registry carries their keys only through
    # the concept map (map_sources below), never the unmapped result text.
    # The constants live in the body: the edit replaces the def and keeps the decorator above it.
    SRC_R1_RESOLUTION = "3_lookup.omop.mapping_resolution"   # INSR R1 (mapping into bronze)
    SRC_R1_USAGE = "3_lookup.omop.mapping_resolution_usage"
    mappings = _lifecycle_source_concept_map()
    usage = (read_source(SRC_R1_USAGE).groupBy("lane_id", "map_key")
             .agg(F.sum("row_count").cast("long").alias("rows"), F.min("first_seen").alias("first_seen"),
                  F.max("last_seen").alias("last_seen")))
    resolved = read_source(SRC_R1_RESOLUTION).join(usage, ["lane_id", "map_key"])
    lane_sources = resolved.where((F.col("component_index") == 1) & F.col("source_code").isNotNull()
                                  & canonical_coding_system(F.coalesce(F.col("source_system"), F.lit("urn:unknown")))
                                  .isin(*R1_REGISTRY_SOURCE_SYSTEMS)).select(
        F.coalesce(F.col("source_system"), F.lit("urn:unknown")).alias("coding_system"),
        F.col("source_code").alias("code"), F.col("source_display").alias("display"),
        F.col("rows").alias("source_use_count"), F.lit(0).cast("long").alias("target_use_count"),
        F.col("first_seen").alias("first_at"), F.col("last_seen").alias("last_at"))
    lane_targets = resolved.where(F.col("target_code").isNotNull()).select(
        F.col("target_system").alias("coding_system"), F.col("target_code").alias("code"),
        F.col("target_display").alias("display"), F.lit(0).cast("long").alias("source_use_count"),
        F.col("rows").alias("target_use_count"), F.col("first_seen").alias("first_at"),
        F.col("last_seen").alias("last_at"))
    map_sources = mappings.select(
        F.col("source_coding_system").alias("coding_system"), F.col("source_code").alias("code"),
        F.col("source_display").alias("display"), F.col("source_row_count").cast("long").alias("source_use_count"),
        F.lit(0).cast("long").alias("target_use_count"), F.col("loaded_at").alias("first_at"),
        F.col("loaded_at").alias("last_at"))
    map_targets = mappings.select(
        F.col("target_coding_system").alias("coding_system"), F.col("target_code").alias("code"),
        F.col("target_display").alias("display"), F.lit(0).cast("long").alias("source_use_count"),
        F.col("source_row_count").cast("long").alias("target_use_count"), F.col("loaded_at").alias("first_at"),
        F.col("loaded_at").alias("last_at"))
    grouped = (
        lane_sources.unionByName(lane_targets).unionByName(map_sources).unionByName(map_targets)
        .withColumn("coding_system", canonical_coding_system(F.col("coding_system")))
        .groupBy("coding_system", "code")
        .agg(
            F.max("display").alias("preferred_display"),
            F.sum("source_use_count").cast("long").alias("source_use_count"),
            F.sum("target_use_count").cast("long").alias("target_use_count"),
            F.min("first_at").alias("first_observed_at"),
            F.max("last_at").alias("last_observed_at"),
        )
    )
    # contract v2: name the deterministic observed-concept identity concept_registry_key
    return grouped.select(
        stable_id("terminology-concept", F.col("coding_system"), F.col("code")).alias("concept_registry_key"),
        "coding_system", "code", "preferred_display",
        F.lit("observed").alias("status"),
        "source_use_count", "target_use_count",
        "first_observed_at", "last_observed_at",
        )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_concept_registry': {'columns': {'code': 'OMOP standard vocabulary concept identifier assigned to '
                                                    'the mapped source value.',
                                            'coding_system': 'Identifier namespace for the observed source '
                                                             'or target code, taken from event or mapping '
                                                             'evidence; event codes with no source namespace '
                                                             'use urn:unknown.',
                                            'concept_registry_key': 'Deterministic SHA-256 observed-concept '
                                                                    'key.',
                                            'first_observed_at': 'Earliest represented bronze observation '
                                                                 'timestamp.',
                                            'last_observed_at': 'Latest represented bronze observation '
                                                                'timestamp.',
                                            'preferred_display': 'Deterministically selected observed '
                                                                 'display.',
                                            'source_use_count': 'Source-side event and mapping observation '
                                                                'count.',
                                            'status': 'Registry observation status.',
                                            'target_use_count': 'Target-side mapping observation count.'},
                                'table': 'Observed source and target coding-system/code pairs across the '
                                         'event index and concept map.'}}
