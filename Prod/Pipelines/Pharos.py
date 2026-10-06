# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
try:
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable
    from datetime import datetime, timedelta
    import uuid
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.types import BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
    from pyspark.sql import functions as F
    from functools import reduce
    import re
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 0, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise

# COMMAND ----------

try:
    # Job parameters. Production execution remains a separate human-controlled rollout.
    dbutils.widgets.dropdown("ENVIRONMENT", "dev", ["dev", "prod"])
    dbutils.widgets.dropdown("FULL_REFRESH", "false", ["false", "true"])
    dbutils.widgets.text("LATENESS_HOURS", "336")
    dbutils.widgets.text("TABLES", "")
    ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT").strip().lower()
    FULL_REFRESH = dbutils.widgets.get("FULL_REFRESH").strip().lower() == "true"
    LATENESS_HOURS = int(dbutils.widgets.get("LATENESS_HOURS"))
    SELECTED_TABLES = {t.strip() for t in dbutils.widgets.get("TABLES").split(",") if t.strip()}
    assert ENVIRONMENT in ("dev", "prod"), ENVIRONMENT
    assert LATENESS_HOURS >= 1, "A positive source lateness overlap is required"
    TARGET_CATALOG = {"dev": "8_dev", "prod": "4_prod"}[ENVIRONMENT]
    TARGET_SCHEMA = "silver"

    def get_target_table(table_name):
        assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name), table_name
        return f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 1, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 2, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    # Barts-only source filters are unchanged. Incrementality is managed per target/source.
    def apply_trust_filter_to_df(df, table_name=None):
        return df.filter(F.col("Trust") == "Barts") if "Trust" in df.columns else df
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 3, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:

    def detect_schema_changes(target_table: str, target_schema: StructType = None, table_comment: str = None):
        """
        Detect what schema changes are needed between current and target schema.
        Returns a dict with all required changes.
        """
        changes = {
            'has_changes': False,
            'columns_to_update': [],
            'columns_to_add': [],
            'table_comment_update': None
        }

        if target_schema:
            current_schema = spark.table(target_table).schema
            current_fields = {f.name: f for f in current_schema.fields}
            target_fields = {f.name for f in target_schema.fields}

            for target_field in target_schema.fields:
                field_name = target_field.name

                if field_name in current_fields:
                    current_field = current_fields[field_name]

                    # Compare type and comment
                    current_comment = current_field.metadata.get("comment", "")
                    target_comment = target_field.metadata.get("comment", "")
                    type_changed = current_field.dataType != target_field.dataType
                    comment_changed = current_comment != target_comment

                    if type_changed or comment_changed:
                        changes['columns_to_update'].append({
                            'name': field_name,
                            'type': target_field.dataType.simpleString(),
                            'comment': target_comment,
                            'type_changed': type_changed,
                            'comment_changed': comment_changed
                        })
                        changes['has_changes'] = True
                else:
                    # New column to add
                    changes['columns_to_add'].append({
                        'name': field_name,
                        'type': target_field.dataType.simpleString(),
                        'comment': target_field.metadata.get("comment", ""),
                        'nullable': target_field.nullable
                    })
                    changes['has_changes'] = True

        if table_comment:
            # Check current table comment
            try:
                current_props = spark.sql(f"SHOW TBLPROPERTIES {target_table}").collect()
                current_comment = next((row.value for row in current_props if row.key == 'comment'), None)

                if current_comment != table_comment:
                    changes['table_comment_update'] = table_comment
                    changes['has_changes'] = True
            except:
                # If we can't get properties, assume update is needed
                changes['table_comment_update'] = table_comment
                changes['has_changes'] = True

        return changes

    # ============================================================================
    # Schema Application
    # ============================================================================
    def escape_comment(text: str) -> str:
        if not text:
            return ""
        return text.replace("\\", "\\\\").replace("'", "''")


    def apply_schema_changes(target_table: str, changes: dict):
        """
        Apply detected schema changes efficiently.
        Minimizes ALTER statements and provides clear feedback.
        """
        updates_applied = []

        # Add new columns
        for col in changes['columns_to_add']:
            sql = f"ALTER TABLE {target_table} ADD COLUMN `{col['name']}` {col['type']}"
            if col['comment']:
                sql += f" COMMENT '{escape_comment(col['comment'])}'"
            spark.sql(sql)
            updates_applied.append(f"Added column {col['name']}")

        # Update existing columns

        # Check if any column has a type change
        type_change_detected = any(col['type_changed'] for col in changes['columns_to_update'])

        if type_change_detected:
            print(f"Type change detected. Recreating table {target_table}...")
            df = spark.table(target_table)
            cols_to_cast = [col['name'] for col in changes['columns_to_update'] if col['type_changed']]
            pre_counts = df.select([F.count(F.col(c)).alias(c) for c in cols_to_cast]).collect()[0].asDict()

            for col_info in changes['columns_to_update']:
                if col_info['type_changed']:
                    current_type = df.schema[col_info['name']].dataType
                    target_type_str = col_info['type']
                    # Handle string → integer types: cast through double first to handle "123.0" or "1.23E5" formats
                    if isinstance(current_type, StringType) and target_type_str in ('bigint', 'int', 'smallint', 'tinyint'):
                        df = df.withColumn(col_info['name'], df[col_info['name']].cast('double').cast(target_type_str))
                    else:
                        df = df.withColumn(col_info['name'], df[col_info['name']].cast(target_type_str))

            # Now check the data loss after changing the data type
            post_counts = df.select([F.count(F.col(c)).alias(c) for c in cols_to_cast]).collect()[0].asDict()
            losses = {c: pre_counts[c] - post_counts[c] for c in cols_to_cast if pre_counts[c] - post_counts[c] > 0}

            audit_rows = [(RUN_ID,target_table,c,pre_counts[c],post_counts[c],pre_counts[c]-post_counts[c]) for c in cols_to_cast]
            spark.createDataFrame(audit_rows,"run_id string, target_table string, column_name string, before_count long, after_count long, lost_count long").withColumn("checked_at",F.current_timestamp()).write.mode("append").saveAsTable(get_target_table("pharos_schema_audit"))

            if losses:
                raise ValueError(f"Data loss detected during cast: {losses}")
            else:
                print("All type casts completed without introducing NULLs.")

            print(f"[PASS] Type migration {target_table}: before={pre_counts}, after={post_counts}, losses={losses}")
            migration_stage = get_target_table("_pharos_schema_migration")
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(migration_stage)
            df = spark.table(migration_stage)
            # Recreate from the staged casts, preserving CDF and row tracking
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("delta.enableChangeDataFeed", "true") \
                .option("delta.enableRowTracking", "true") \
                .saveAsTable(target_table)

            spark.sql(f"DROP TABLE {migration_stage}")
            updates_applied.append(f"Recreated {target_table} due to type change")

        # Handle column comment updates separately
        comment_change_detected = any(col['comment_changed'] for col in changes['columns_to_update'])

        if comment_change_detected:
            for col in changes['columns_to_update']:
                if col['comment_changed']:
                    spark.sql(f"""
                        ALTER TABLE {target_table}
                        ALTER COLUMN `{col['name']}` COMMENT '{escape_comment(col['comment'])}'
                    """)
                    updates_applied.append(f"Updated {col['name']} comment")

        # Update table comment
        if changes['table_comment_update']:
            spark.sql(f"""
                ALTER TABLE {target_table}
                SET TBLPROPERTIES ('comment' = '{escape_comment(changes['table_comment_update'])}')
            """)
            updates_applied.append("Updated table comment")

        if updates_applied:
            print(f"[INFO] Applied {len(updates_applied)} updates to {target_table}:")
            for i, update in enumerate(updates_applied):
                if i < 5:  # Show first 5 changes
                    print(f"  - {update}")
                elif i == 5:
                    print(f"  ... and {len(updates_applied) - 5} more")
                    break

    # ============================================================================
    # Table Creation
    # ============================================================================

    def create_table_with_schema(source_df, target_table: str, target_schema: StructType = None, table_comment: str = None):
        """
        Create a new Delta table with schema and metadata.
        """
        if target_schema:
            # 1. Create the empty table first
            builder = (DeltaTable.createIfNotExists(spark)
                      .tableName(target_table)
                      .addColumns(target_schema))

            if table_comment:
                builder = builder.comment(table_comment)

            builder = (builder
                      .property("delta.enableChangeDataFeed", "true")
                      .property("delta.enableRowTracking", "true"))
            builder.execute()
            print(f"[INFO] Created table {target_table} with schema and metadata")

            # 2. Enforce schema using Select/Cast (Avoiding RDD conversion)
            # This keeps the operation inside the JVM and preserves Catalyst optimizations
            select_expr = []
            for field in target_schema.fields:
                if field.name in source_df.columns:
                    select_expr.append(F.col(field.name).cast(field.dataType))
                else:
                    # Handle missing columns if necessary, or let it fail depending on requirements
                    select_expr.append(F.lit(None).cast(field.dataType).alias(field.name))

            source_df_aligned = source_df.select(*select_expr)

            # 3. Append data
            source_df_aligned.write.mode("append").saveAsTable(target_table)

            apply_column_comments(target_table, target_schema)
        else:
            # Fallback for no schema
            (source_df.write
                      .format("delta")
                      .option("delta.enableChangeDataFeed", "true")
                      .mode("overwrite")
                      .saveAsTable(target_table))

    def apply_column_comments(target_table: str, schema: StructType):
        """Helper to apply column comments to a newly created table."""
        comments_applied = 0
        for field in schema.fields:
            if "comment" in field.metadata and field.metadata["comment"]:
                spark.sql(f"""
                    ALTER TABLE {target_table}
                    ALTER COLUMN `{field.name}`
                    COMMENT '{escape_comment(field.metadata["comment"])}'
                """)
                comments_applied += 1

        if comments_applied > 0:
            print(f"[INFO] Applied {comments_applied} column comments to {target_table}")
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 4, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    # ============================================================================
    # Main Update Function
    # ============================================================================
    def align_to_target(source_df, target_table: str):
        """
        Align source_df to the column set of target_table.

        Delta's whenMatchedUpdateAll()/whenNotMatchedInsertAll() require the source to
        carry every target column, so a declared column that no builder populates aborts
        the merge. Fill those with typed NULLs, drop anything the target does not have,
        and report both so the builder can be corrected.
        """
        target_fields = spark.table(target_table).schema.fields
        target_names = [f.name for f in target_fields]
        src_names = set(source_df.columns)

        missing = [n for n in target_names if n not in src_names]
        extra = [c for c in source_df.columns if c not in target_names]

        if missing:
            print(f"[WARN] {target_table}: source is missing target columns {missing} - filling with NULL")
        if extra:
            print(f"[WARN] {target_table}: source has columns not in target {extra} - dropping")
        if not missing and not extra:
            return source_df

        return source_df.select(*[
            source_df[f.name] if f.name in src_names
            else F.lit(None).cast(f.dataType).alias(f.name)
            for f in target_fields
        ])


    # Source-time checkpoints, with one row per target/source. date_checked is audit only.
    # Re-read a configurable overlap (default 14 days); sources without source time trigger
    # a full person rebuild and are reported explicitly, never silently checkpointed.
    _BUILD = None
    _SOURCE_SNAPSHOTS = {}
    RUN_ID = str(uuid.uuid4())
    CHECKPOINT_TABLE = get_target_table('pharos_source_watermarks')
    AUDIT_TABLE = get_target_table('pharos_mapping_audit')


    def begin_build(target, dependencies):
        global _BUILD
        assert target.startswith(TARGET_CATALOG + '.'), target
        old = {}
        if table_exists(CHECKPOINT_TABLE) and not FULL_REFRESH:
            old = {r.source_table: r for r in spark.table(CHECKPOINT_TABLE).filter(F.col('target_table') == target).collect()}
        sources, changed, bounds = {}, [], []
        force_full = FULL_REFRESH or not table_exists(target)
        for name in dependencies:
            if name not in _SOURCE_SNAPSHOTS:
                # Freeze Delta inputs to a version so rows and their high watermark agree.
                # A view has no Delta history: retain the full-refresh fallback below.
                source = spark.table(name)
                version = None
                try:
                    version = spark.sql(f'DESCRIBE HISTORY {name} LIMIT 1').first().version
                    source = spark.read.option('versionAsOf', version).table(name)
                except Exception:
                    pass
                _SOURCE_SNAPSHOTS[name] = (source, version)
            source, version = _SOURCE_SNAPSHOTS[name]
            columns = {c.lower(): c for c in source.columns}
            person = columns.get('person_id') or columns.get('motherperson_id') or columns.get('personid')
            assert person, f'No person key registered for dependency {name}'
            time = columns.get('adc_updt')
            high = source.agg(F.max(F.col(time))).first()[0] if time else None
            # Future source clocks must not move the checkpoint beyond this observation.
            if high is not None: high = min(high, datetime.utcnow())
            checkpoint = old.get(name)
            previous = checkpoint.source_watermark if checkpoint else None
            previous_version = checkpoint.source_version if checkpoint else None
            # Derived silver ADC_UPDT is not a complete dependency clock. A changed
            # version rebuilds its dependants; never predicate on date_checked.
            if name.startswith(TARGET_CATALOG + '.silver.pharos_') and previous_version != version:
                force_full = True
            if time is None or version is None:
                force_full = True
                print(f'[INFO] {target}: full rebuild because {name} has no snapshot/source-time contract')
            if previous is None:
                force_full = True
            sources[name] = (source, person)
            if time and previous is not None:
                cutoff = previous - timedelta(hours=LATENESS_HOURS)
                touched = source.filter((F.col(time) > F.lit(cutoff)) | F.col(time).isNull())
                changed.append(touched.select(F.col(person).cast('long').alias('person_id')))
            # CDF captures arbitrarily late arrivals, removals and old/new person keys.
            # This matters for medication remapping commits whose ADC_UPDT can be years old.
            if previous_version is not None and version is not None and version > previous_version:
                try:
                    feed = (spark.read.option('readChangeFeed','true')
                        .option('startingVersion',previous_version + 1).option('endingVersion',version).table(name))
                    feed.take(1)  # Validate retained CDF before constructing a lazy union.
                    changed.append(feed.select(F.col(person).cast('long').alias('person_id')))
                except Exception as error:
                    force_full = True
                    print(f'[INFO] {target}: full rebuild; CDF unavailable for changed source {name}: {type(error).__name__}')
            bounds.append((target, name, high, version, RUN_ID))
        persons = None
        if not force_full:
            persons = reduce(lambda a,b: a.unionByName(b), changed).filter('person_id IS NOT NULL').distinct()
        _BUILD = {'target': target, 'sources': sources, 'persons': persons, 'bounds': bounds, 'full': force_full}


    def source_table(name):
        source, person = _BUILD['sources'][name]
        if _BUILD['persons'] is not None:
            source = source.join(_BUILD['persons'].select(F.col('person_id').alias('_changed_person')),
                                 source[person] == F.col('_changed_person'), 'left_semi')
        if 'SNOMED_CODE' in source.columns:
            source = upstream_provenance(source, name)
        return source


    def deterministic_rows(df, keys):
        """Prefer populated evidence, then newest source time, then explicit case/full-row tie."""
        names = sorted(c for c in df.columns if c not in ('created_at','updated_at','date_checked'))
        has_data = reduce(lambda a,b: a+b, [F.col(c).isNotNull().cast('int') for c in names], F.lit(0))
        payload = F.to_json(F.struct(*[F.col(c) for c in names]))
        order = [has_data.desc_nulls_last()]
        if 'ADC_UPDT' in df.columns: order.append(F.col('ADC_UPDT').desc_nulls_last())
        order += [(payload == F.lower(payload)).desc_nulls_last(), payload.asc_nulls_last()]
        return (df.withColumn('_row_choice', F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
                .filter('_row_choice = 1').drop('_row_choice'))


    def validate_mapping_output(df, target):
        """Per-field assertions plus counts, including the expected negative outcomes."""
        expressions = [F.count('*').alias('row_count')]
        prefixes = [c[:-4] for c in df.columns if c.endswith('snomed_code')]
        for i,prefix in enumerate(prefixes):
            expressions += [F.count(prefix+'code').alias(f'mapped_{i}'),
                F.sum(F.when(F.col(prefix+'code').isNotNull() & F.col(prefix+'method').isNull(),1).otherwise(0)).alias(f'missing_method_{i}'),
                F.sum(F.when(F.col(prefix+'code').isNull() & F.col(prefix+'rejection_reason').isNull(),1).otherwise(0)).alias(f'missing_reason_{i}')]
        values = df.agg(*expressions).first().asDict()
        for column, category in VALUESET_FIELDS.get(target.rsplit('.',1)[-1], []):
            if column not in df.columns: continue
            lookup = spark.table(get_target_table('pharos_snomed_lookup')).filter(F.col('category') == category)
            observed = df.alias('d').join(lookup.alias('l'), F.col('d.'+column) == F.col('l.source_label'), 'left')
            mismatch = observed.filter(~F.col('d.'+column+'_snomed_code').eqNullSafe(F.col('l.concept_code')))
            assert not mismatch.take(1), f'{target}.{column}: mapping differs from per-label expected outcome'
            expected = observed.filter(F.col('l.concept_code').isNotNull()).count()
            actual = values[f'mapped_{prefixes.index(column+"_snomed_")}']
            assert actual == expected, f'{target}.{column}: expected {expected} mapped rows, got {actual}'

        rows = []
        for i,prefix in enumerate(prefixes):
            assert not values[f'missing_method_{i}'], f'{target}.{prefix}: published mapping lacks provenance'
            assert not values[f'missing_reason_{i}'], f'{target}.{prefix}: unmapped result lacks reason'
            rows.append((RUN_ID,target,prefix,values['row_count'],values[f'mapped_{i}']))
        if target.endswith('.pharos_medical_history'):
            assert values['row_count'] == df.select('person_id').distinct().count(), 'Family-history person fan-out'
        if rows:
            (spark.createDataFrame(rows,'run_id string, target_table string, field string, row_count long, mapped_count long')
                .withColumn('checked_at',F.current_timestamp()).write.mode('append').saveAsTable(AUDIT_TABLE))
        return values['row_count']
    
    def apply_sharing_tags(table_name, internal_columns=None):
        """
        Apply sharing tags to a table.

        Default internal columns:
        - person_id
        - ADC_UPDT
        - *snomed_method
        - *snomed_similarity
        - *snomed_source
        - *snomed_model
        - *snomed_model_version
        - *snomed_rule_id
        - *snomed_rejection_reason

        Additional internal columns can be supplied per table.
        """

        internal_columns = set(internal_columns or [])

        df = spark.table(table_name)

        for col_name in df.columns:

            is_internal = (
                col_name == "person_id"
                or col_name == "ADC_UPDT"
                or col_name in internal_columns
                or col_name.endswith("snomed_method")
                or col_name.endswith("snomed_similarity")
                or col_name.endswith("snomed_source")
                or col_name.endswith("snomed_model")
                or col_name.endswith("snomed_model_version")
                or col_name.endswith("snomed_rule_id")
                or col_name.endswith("snomed_rejection_reason")
            )

            tag_value = "BARTS_INTERNAL" if is_internal else "PHAROS_EXPORT"

            spark.sql(f"""
                ALTER TABLE {table_name}
                ALTER COLUMN `{col_name}`
                SET TAGS ('sharing' = '{tag_value}')
            """)

    def update_table(source_df, target_table, index_columns, target_schema=None, table_comment=None,internal_columns=None):
        assert _BUILD['target'] == target_table, 'Builder/target contract mismatch'
        keys = [index_columns] if isinstance(index_columns,str) else list(index_columns)
        # Declare every mapping before align_to_target can hide a projection bug.
        missing = [f.name for f in target_schema if 'snomed_' in f.name and f.name not in source_df.columns]
        assert not missing, f'{target_table}: missing mapping columns {missing}'
        source_df = deterministic_rows(source_df, keys)
        # Materialise once: no cache/persist and no source plan reads from the table being merged.
        stage = get_target_table('_pharos_stage_' + target_table.rsplit('.',1)[-1])
        aligned = source_df.select(*[F.col(f.name).cast(f.dataType).alias(f.name, metadata=f.metadata)
            if f.name in source_df.columns else F.lit(None).cast(f.dataType).alias(f.name) for f in target_schema])
        aligned.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(stage)
        source_df = spark.table(stage)
        count = validate_mapping_output(source_df,target_table)
        if table_exists(target_table):
            changes = detect_schema_changes(target_table,target_schema,table_comment)
            if changes['has_changes']: apply_schema_changes(target_table,changes)
            source_df = align_to_target(source_df,target_table)
            if _BUILD['full']:
                # A full backfill replaces the entire declared population atomically.
                # This also removes legacy duplicate target keys, which MERGE alone
                # would update in place and retain. All validation precedes the write.
                assert not source_df.groupBy(*keys).count().filter('count > 1').take(1), 'Unsafe full refresh: duplicate source keys'
                (source_df.write.mode('overwrite').option('overwriteSchema','true')
                    .option('delta.enableChangeDataFeed','true').option('delta.enableRowTracking','true')
                    .saveAsTable(target_table))
            else:
                existing = spark.table(target_table)
                scoped = existing if _BUILD['full'] else existing.join(_BUILD['persons'],'person_id','left_semi')
                # Tombstones remove obsolete rows only for rebuilt persons, in the same MERGE.
                condition = reduce(lambda a,b:a & b,[F.col('t.'+k).eqNullSafe(F.col('s.'+k)) for k in keys])
                obsolete = scoped.alias('t').join(source_df.alias('s'),condition,'left_anti')
                obsolete_count = obsolete.count()
                assert obsolete_count <= scoped.count(), 'Delete scope exceeds rebuilt persons'
                merge_source = source_df.withColumn('_delete',F.lit(False)).unionByName(obsolete.withColumn('_delete',F.lit(True)))
                # Persist tombstones too, preventing self-read/merge surprises and duplicate evaluation.
                merge_stage = get_target_table('_pharos_merge_' + target_table.rsplit('.',1)[-1])
                merge_source.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(merge_stage)
                merge_source = spark.table(merge_stage)
                assert not merge_source.groupBy(*keys).count().filter('count > 1').take(1), 'Unsafe MERGE: duplicate keys'
                mapping = {c:'s.`'+c+'`' for c in source_df.columns}
                if 'created_at' in mapping: mapping['created_at'] = 'coalesce(t.created_at,s.created_at)'
                merge_condition = ' AND '.join(f't.`{c}` <=> s.`{c}`' for c in keys)
                (DeltaTable.forName(spark,target_table).alias('t').merge(merge_source.alias('s'),merge_condition)
                    .whenMatchedDelete(condition='s._delete')
                    .whenMatchedUpdate(condition='NOT s._delete',set=mapping)
                    .whenNotMatchedInsert(condition='NOT s._delete',values={c:'s.`'+c+'`' for c in source_df.columns})
                    .execute())
                spark.sql(f'DROP TABLE {merge_stage}')
        else:
            create_table_with_schema(source_df,target_table,target_schema,table_comment)
        # Advance only after the target MERGE succeeds. A failed checkpoint simply replays.
        checkpoints = (spark.createDataFrame(_BUILD['bounds'],
            'target_table string, source_table string, source_watermark timestamp, source_version long, run_id string')
            .withColumn('completed_at',F.current_timestamp()))
        if table_exists(CHECKPOINT_TABLE):
            (DeltaTable.forName(spark,CHECKPOINT_TABLE).alias('t').merge(checkpoints.alias('s'),
                't.target_table=s.target_table AND t.source_table=s.source_table')
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        else: checkpoints.write.saveAsTable(CHECKPOINT_TABLE)
        apply_sharing_tags(target_table)
        spark.sql(f'DROP TABLE {stage}')
        # A target used later in this run must be read after its successful update.
        _SOURCE_SNAPSHOTS.pop(target_table,None)
        print(f'[PASS] {target_table}: rebuilt {count} rows; full_refresh={_BUILD["full"]}')
        

except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 5, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    # SNOMED semantic record. Terms are ALWAYS derived from the loaded vocabulary.
    # Columns: category, source_label, concept_code, maps_to_null, ancestor_code,
    # allowed_domains, rejection_reason. A null is an explicit mapping outcome.
    # Lobular neoplasia spans ALH and LCIS: no single loaded concept preserves that assertion.
    # Nuclear medicine is broader than imaging; use its own Procedure branch.
    LOOKUP_ROWS = [('sex', '001 - Female', '248152002', False, None, 'Observation', None),
        ('sex', '002 - Male', '248153007', False, None, 'Observation', None),
        ('ethnic_group', '011 White - British', '976631000000101', False, None, 'Race', None),
        ('ethnic_group', '012 White - Irish', '976651000000108', False, None, 'Race', None),
        ('ethnic_group', '013 White - White Other', '976691000000100', False, None, 'Race', None),
        ('ethnic_group', '021 Mixed - White and Black Caribbean', '976711000000103', False, None, 'Race', None),
        ('ethnic_group', '022 Mixed - White and Black African', '976731000000106', False, None, 'Race', None),
        ('ethnic_group', '023 Mixed - White and Asian', '976751000000104', False, None, 'Race', None),
        ('ethnic_group', '024 Mixed - Other', '976771000000108', False, None, 'Race', None),
        ('ethnic_group', '031 Asian - Indian', '976791000000107', False, None, 'Race', None),
        ('ethnic_group', '032 Asian - Pakistani', '976811000000108', False, None, 'Race', None),
        ('ethnic_group', '033 Asian - Bangladeshi', '976831000000100', False, None, 'Race', None),
        ('ethnic_group', '034 Asian - Other', '976871000000103', False, None, 'Race', None),
        ('ethnic_group', '041 Black - Caribbean', '976911000000101', False, None, 'Race', None),
        ('ethnic_group', '042 Black - African', '976891000000104', False, None, 'Race', None),
        ('ethnic_group', '043 Black - Other', '976931000000109', False, None, 'Race', None),
        ('ethnic_group', '051 Other - Chinese', '976851000000107', False, None, 'Race', None),
        ('ethnic_group', '054 Other', '976971000000106', False, None, 'Race', None),
        ('ethnic_group', 'G09 Unknown', None, True, None, 'Race', 'unknown_or_not_recorded'),
        ('imaging_type', '000 - Mammogram', '71651007', False, '363679005', 'Procedure', None),
        ('imaging_type', '001 - Ultrasound', '16310003', False, '363679005', 'Procedure', None),
        ('imaging_type', '002 - PET', '82918005', False, '363679005', 'Procedure', None),
        ('imaging_type', '003 - CT', '77477000', False, '363679005', 'Procedure', None),
        ('imaging_type', '004 - MRI', '113091000', False, '363679005', 'Procedure', None),
        ('imaging_type', '005 - Tomosynthesis', '450566007', False, '363679005', 'Procedure', None),
        ('imaging_type', '006 - X-ray', '168537006', False, '363679005', 'Procedure', None),
        ('imaging_type', '007 - Nuclear medicine (NM)', '371572003', False, '371572003', 'Procedure', None),
        ('imaging_type', 'G09 - Unknown', None, True, '363679005', 'Procedure', 'unknown_or_not_recorded'),
        ('relation_degree', '000 - None', None, True, '125679009', 'Relationship', 'unknown_or_not_recorded'),
        ('relation_degree', '001 - 1st degree', '125678001', False, '125679009', 'Relationship', None),
        ('relation_degree', '002 - 2nd degree', '699110007', False, '125679009', 'Relationship', None),
        ('relation_degree', '003 - 3rd degree', '1269487002', False, '125679009', 'Relationship', None),
        ('relation_degree', 'G09 - Unknown', None, True, '125679009', 'Relationship', 'unknown_or_not_recorded'),
        ('metastasis_site', 'A25 - Distant lymph node', '59441001', False, '59441001', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A02 - Lung', '39607008', False, '39607008', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A10 - Liver', '10200004', False, '10200004', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A16 - Renal', '64033007', False, '64033007', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A06 - Bone', '272673000', False, '272673000', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A07 - Brain', '12738006', False, '12738006', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A19 - Leptomeningeal', '66697007', False, '66697007', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A20 - Viscera', '362937008', False, '362937008', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A12 - Ovary', '15497006', False, '15497006', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A15 - Pleura', '3120008', False, '3120008', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A14 - Peritoneum', '15425007', False, '15425007', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A18 - Mediastinum', '72410000', False, '72410000', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A21 - Abdominal/gastrointestinal', '122865005', False, '122865005', 'Spec Anatomic Site', None),
        ('metastasis_site', 'A17 - Skin', '39937001', False, '39937001', 'Spec Anatomic Site', None),
        ('metastasis_site', 'G03 - Other metastasis', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('metastasis_site', 'G09 - Unknown', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('vital_status', '000 - Alive', '438949009', False, None, 'Observation', None),
        ('vital_status', '001 - Deceased', '419099009', False, None, 'Observation', None),
        ('vital_status', 'G09 - Unknown', None, True, None, 'Observation', 'unknown_or_not_recorded'),
        ('imaging_site', 'A05 - Abdomen', '818983003', False, '818983003', 'Spec Anatomic Site', None),
        ('imaging_site', 'A22 - Axilla', '91470000', False, '91470000', 'Spec Anatomic Site', None),
        ('imaging_site', 'A06 - Bone', '272673000', False, '272673000', 'Spec Anatomic Site', None),
        ('imaging_site', 'A07 - Brain', '12738006', False, '12738006', 'Spec Anatomic Site', None),
        ('imaging_site', 'A01 - Breast', '76752008', False, '76752008', 'Spec Anatomic Site', None),
        ('imaging_site', 'A09 - Chest / Thorax', '51185008', False, '51185008', 'Spec Anatomic Site', None),
        ('imaging_site', 'A10 - Liver', '10200004', False, '10200004', 'Spec Anatomic Site', None),
        ('imaging_site', 'A11 - Neck', '45048000', False, '45048000', 'Spec Anatomic Site', None),
        ('imaging_site', 'A13 - Pelvis', '12921003', False, '12921003', 'Spec Anatomic Site', None),
        ('imaging_site', 'A26 - Sentinel node', '59441001', False, '59441001', 'Spec Anatomic Site', None),
        ('imaging_site', 'G03 - Other', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('imaging_site', 'G09 - Unknown', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('path_atypical', '000 - Atypical Ductal Hyperplasia (ADH)', '427785007', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '001 - Atypical Lobular Hyperplasia (ALH)', '450697004', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '002 - Atypical Intraduct Epithelial Proliferation (AIDEP)', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_atypical', '003 - Columnar Cell Change with Atypia', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_atypical', '004 - Columnar Cell Hyperplasia With Atypia', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_atypical', '005 - Flat Epithelial Atypia (FEA)', '860895001', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '006 - Lobular Neoplasia', None, True, '116339002', 'Condition|Observation', 'no_specific_loaded_concept_for_ALH_LCIS_umbrella'),
        ('path_atypical', '007 - Pagets Disease', '403946000', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '008 - Papilloma / Papillary Lesion / Sclerosing Papillary Lesion', '99571000119102', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '009 - Radial Scar or Complex Sclerosing Lesion', '390787006', False, '116339002', 'Condition|Observation', None),
        ('path_atypical', '010 - None recorded', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('path_atypical', 'G09 - Unknown', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('path_benign', '000 - Apocrine Metaplasia', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '001 - Benign', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '002 - Chemotherapy Effect (CHEM)', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('path_benign', '003 - Columnar Cell Change', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '004 - Columnar Cell Hyperplasia', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '005 - Cystic Change', '399294002', False, '116339002', 'Condition|Observation', None),
        ('path_benign', '006 - Epithelial Hyperplasia of Usual Type', '472905007', False, '116339002', 'Condition|Observation', None),
        ('path_benign', '007 - Fibroadenoma (FAD)', '254845004', False, '116339002', 'Condition|Observation', None),
        ('path_benign', '008 - Fibroadenomatoid Change', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '009 - Fibrocystic Change', '27431007', False, '116339002', 'Condition|Observation', None),
        ('path_benign', '010 - Fibrosis', '29070004', False, '116339002', 'Condition|Observation', None),
        ('path_benign', '011 - Microcystic Change', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '012 - Microglandular Adenosis', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '013 - Pseudoangiomatous Stromal Hyperplasia (PASH)', None, True, '116339002', 'Condition|Observation', 'no_defensible_loaded_concept'),
        ('path_benign', '014 - Sclerosing Adenosis', '105261000119101', False, '116339002', 'Condition|Observation', None),
        ('path_benign', 'G03 - Other', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('path_benign', '016 - Not recorded', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('path_benign', 'G09 - Unknown', None, True, '116339002', 'Condition|Observation', 'unknown_or_not_recorded'),
        ('tissue_site', 'A01 - Breast', '76752008', False, '76752008', 'Spec Anatomic Site', None),
        ('tissue_site', 'A23 - Axillary lymph node', '68171009', False, '68171009', 'Spec Anatomic Site', None),
        ('tissue_site', 'A08 - Chest wall', '78904004', False, '78904004', 'Spec Anatomic Site', None),
        ('tissue_site', 'A10 - Liver', '10200004', False, '10200004', 'Spec Anatomic Site', None),
        ('tissue_site', 'A07 - Brain', '12738006', False, '12738006', 'Spec Anatomic Site', None),
        ('tissue_site', 'A02 - Lung', '39607008', False, '39607008', 'Spec Anatomic Site', None),
        ('tissue_site', 'A06 - Bone', '272673000', False, '272673000', 'Spec Anatomic Site', None),
        ('tissue_site', 'A27 - Other locoregional lymph node', '59441001', False, '59441001', 'Spec Anatomic Site', None),
        ('tissue_site', 'A24 - SCF node', '76838003', False, '76838003', 'Spec Anatomic Site', None),
        ('tissue_site', 'A25 - Distant lymph node', '59441001', False, '59441001', 'Spec Anatomic Site', None),
        ('tissue_site', 'A28 - Lymph node - unknown region', '59441001', False, '59441001', 'Spec Anatomic Site', None),
        ('tissue_site', '008 - Other', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('tissue_site', '009 - Unknown', None, True, None, 'Spec Anatomic Site', 'unknown_or_not_recorded'),
        ('sex', 'G09 Unknown', None, True, None, 'Observation', 'unknown_or_not_recorded'),
        ('treatment_type', 'chemotherapy', '367336001', False, '71388002', 'Procedure', None),
        ('treatment_type', '002 - Chemotherapy', '367336001', False, '71388002', 'Procedure', None),
        ('treatment_type', '001 - Endocrine', '169413002', False, '71388002', 'Procedure', None),
        ('treatment_type', '003 - Antibody', '76334006', False, '71388002', 'Procedure', None),
        ('treatment_type', '004 - CDK4/6 Inhibitor', None, True, '71388002', 'Procedure', 'no_specific_loaded_procedure_concept'),
        ('treatment_type', '006 - PARP Inhibitor', None, True, '71388002', 'Procedure', 'no_specific_loaded_procedure_concept'),
        ('treatment_type', '006 - Radiotherapy', '1287742003', False, '71388002', 'Procedure', None)]

    # Review these pins whenever changing a mapping; do not derive them at runtime.
    EXPECTED_CODES = {
        ('sex', '001 - Female'): '248152002',
        ('sex', '002 - Male'): '248153007',
        ('ethnic_group', '011 White - British'): '976631000000101',
        ('ethnic_group', '012 White - Irish'): '976651000000108',
        ('ethnic_group', '013 White - White Other'): '976691000000100',
        ('ethnic_group', '021 Mixed - White and Black Caribbean'): '976711000000103',
        ('ethnic_group', '022 Mixed - White and Black African'): '976731000000106',
        ('ethnic_group', '023 Mixed - White and Asian'): '976751000000104',
        ('ethnic_group', '024 Mixed - Other'): '976771000000108',
        ('ethnic_group', '031 Asian - Indian'): '976791000000107',
        ('ethnic_group', '032 Asian - Pakistani'): '976811000000108',
        ('ethnic_group', '033 Asian - Bangladeshi'): '976831000000100',
        ('ethnic_group', '034 Asian - Other'): '976871000000103',
        ('ethnic_group', '041 Black - Caribbean'): '976911000000101',
        ('ethnic_group', '042 Black - African'): '976891000000104',
        ('ethnic_group', '043 Black - Other'): '976931000000109',
        ('ethnic_group', '051 Other - Chinese'): '976851000000107',
        ('ethnic_group', '054 Other'): '976971000000106',
        ('ethnic_group', 'G09 Unknown'): None,
        ('imaging_type', '000 - Mammogram'): '71651007',
        ('imaging_type', '001 - Ultrasound'): '16310003',
        ('imaging_type', '002 - PET'): '82918005',
        ('imaging_type', '003 - CT'): '77477000',
        ('imaging_type', '004 - MRI'): '113091000',
        ('imaging_type', '005 - Tomosynthesis'): '450566007',
        ('imaging_type', '006 - X-ray'): '168537006',
        ('imaging_type', '007 - Nuclear medicine (NM)'): '371572003',
        ('imaging_type', 'G09 - Unknown'): None,
        ('relation_degree', '000 - None'): None,
        ('relation_degree', '001 - 1st degree'): '125678001',
        ('relation_degree', '002 - 2nd degree'): '699110007',
        ('relation_degree', '003 - 3rd degree'): '1269487002',
        ('relation_degree', 'G09 - Unknown'): None,
        ('metastasis_site', 'A25 - Distant lymph node'): '59441001',
        ('metastasis_site', 'A02 - Lung'): '39607008',
        ('metastasis_site', 'A10 - Liver'): '10200004',
        ('metastasis_site', 'A16 - Renal'): '64033007',
        ('metastasis_site', 'A06 - Bone'): '272673000',
        ('metastasis_site', 'A07 - Brain'): '12738006',
        ('metastasis_site', 'A19 - Leptomeningeal'): '66697007',
        ('metastasis_site', 'A20 - Viscera'): '362937008',
        ('metastasis_site', 'A12 - Ovary'): '15497006',
        ('metastasis_site', 'A15 - Pleura'): '3120008',
        ('metastasis_site', 'A14 - Peritoneum'): '15425007',
        ('metastasis_site', 'A18 - Mediastinum'): '72410000',
        ('metastasis_site', 'A21 - Abdominal/gastrointestinal'): '122865005',
        ('metastasis_site', 'A17 - Skin'): '39937001',
        ('metastasis_site', 'G03 - Other metastasis'): None,
        ('metastasis_site', 'G09 - Unknown'): None,
        ('vital_status', '000 - Alive'): '438949009',
        ('vital_status', '001 - Deceased'): '419099009',
        ('vital_status', 'G09 - Unknown'): None,
        ('imaging_site', 'A05 - Abdomen'): '818983003',
        ('imaging_site', 'A22 - Axilla'): '91470000',
        ('imaging_site', 'A06 - Bone'): '272673000',
        ('imaging_site', 'A07 - Brain'): '12738006',
        ('imaging_site', 'A01 - Breast'): '76752008',
        ('imaging_site', 'A09 - Chest / Thorax'): '51185008',
        ('imaging_site', 'A10 - Liver'): '10200004',
        ('imaging_site', 'A11 - Neck'): '45048000',
        ('imaging_site', 'A13 - Pelvis'): '12921003',
        ('imaging_site', 'A26 - Sentinel node'): '59441001',
        ('imaging_site', 'G03 - Other'): None,
        ('imaging_site', 'G09 - Unknown'): None,
        ('path_atypical', '000 - Atypical Ductal Hyperplasia (ADH)'): '427785007',
        ('path_atypical', '001 - Atypical Lobular Hyperplasia (ALH)'): '450697004',
        ('path_atypical', '002 - Atypical Intraduct Epithelial Proliferation (AIDEP)'): None,
        ('path_atypical', '003 - Columnar Cell Change with Atypia'): None,
        ('path_atypical', '004 - Columnar Cell Hyperplasia With Atypia'): None,
        ('path_atypical', '005 - Flat Epithelial Atypia (FEA)'): '860895001',
        ('path_atypical', '006 - Lobular Neoplasia'): None,
        ('path_atypical', '007 - Pagets Disease'): '403946000',
        ('path_atypical', '008 - Papilloma / Papillary Lesion / Sclerosing Papillary Lesion'): '99571000119102',
        ('path_atypical', '009 - Radial Scar or Complex Sclerosing Lesion'): '390787006',
        ('path_atypical', '010 - None recorded'): None,
        ('path_atypical', 'G09 - Unknown'): None,
        ('path_benign', '000 - Apocrine Metaplasia'): None,
        ('path_benign', '001 - Benign'): None,
        ('path_benign', '002 - Chemotherapy Effect (CHEM)'): None,
        ('path_benign', '003 - Columnar Cell Change'): None,
        ('path_benign', '004 - Columnar Cell Hyperplasia'): None,
        ('path_benign', '005 - Cystic Change'): '399294002',
        ('path_benign', '006 - Epithelial Hyperplasia of Usual Type'): '472905007',
        ('path_benign', '007 - Fibroadenoma (FAD)'): '254845004',
        ('path_benign', '008 - Fibroadenomatoid Change'): None,
        ('path_benign', '009 - Fibrocystic Change'): '27431007',
        ('path_benign', '010 - Fibrosis'): '29070004',
        ('path_benign', '011 - Microcystic Change'): None,
        ('path_benign', '012 - Microglandular Adenosis'): None,
        ('path_benign', '013 - Pseudoangiomatous Stromal Hyperplasia (PASH)'): None,
        ('path_benign', '014 - Sclerosing Adenosis'): '105261000119101',
        ('path_benign', 'G03 - Other'): None,
        ('path_benign', '016 - Not recorded'): None,
        ('path_benign', 'G09 - Unknown'): None,
        ('tissue_site', 'A01 - Breast'): '76752008',
        ('tissue_site', 'A23 - Axillary lymph node'): '68171009',
        ('tissue_site', 'A08 - Chest wall'): '78904004',
        ('tissue_site', 'A10 - Liver'): '10200004',
        ('tissue_site', 'A07 - Brain'): '12738006',
        ('tissue_site', 'A02 - Lung'): '39607008',
        ('tissue_site', 'A06 - Bone'): '272673000',
        ('tissue_site', 'A27 - Other locoregional lymph node'): '59441001',
        ('tissue_site', 'A24 - SCF node'): '76838003',
        ('tissue_site', 'A25 - Distant lymph node'): '59441001',
        ('tissue_site', 'A28 - Lymph node - unknown region'): '59441001',
        ('tissue_site', '008 - Other'): None,
        ('tissue_site', '009 - Unknown'): None,
        ('sex', 'G09 Unknown'): None,
        ('treatment_type', 'chemotherapy'): '367336001',
        ('treatment_type', '002 - Chemotherapy'): '367336001',
        ('treatment_type', '001 - Endocrine'): '169413002',
        ('treatment_type', '003 - Antibody'): '76334006',
        ('treatment_type', '004 - CDK4/6 Inhibitor'): None,
        ('treatment_type', '006 - PARP Inhibitor'): None,
        ('treatment_type', '006 - Radiotherapy'): '1287742003'}

    DEMOGRAPHIC_SEX_ROWS = [(362, '001 - Female'),
        (363, '002 - Male')]
    DEMOGRAPHIC_ETHNICITY_ROWS = [(3767643, '010 - White', '011 White - British'),
        (3767645, '010 - White', '012 White - Irish'),
        (3767650, '010 - White', '013 White - White Other'),
        (3767654, '020 - Mixed', '021 Mixed - White and Black Caribbean'),
        (3767653, '020 - Mixed', '022 Mixed - White and Black African'),
        (3767652, '020 - Mixed', '023 Mixed - White and Asian'),
        (3767649, '020 - Mixed', '024 Mixed - Other'),
        (3767644, '030 - Asian', '031 Asian - Indian'),
        (3767651, '030 - Asian', '032 Asian - Pakistani'),
        (3767640, '030 - Asian', '033 Asian - Bangladeshi'),
        (3767647, '030 - Asian', '034 Asian - Other'),
        (3767641, '040 - Black', '041 Black - Caribbean'),
        (3767638, '040 - Black', '042 Black - African'),
        (3767648, '040 - Black', '043 Black - Other'),
        (3767642, '050 - Other', '051 Other - Chinese'),
        (3767639, '050 - Other', '054 Other'),
        (312508, '050 - Other', '054 Other'),
        (0, 'G09 - Unknown', 'G09 Unknown'),
        (3767646, 'G09 - Unknown', 'G09 Unknown')]
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 6, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    # Common mapping provenance, carried as a struct until the final projection.

    def upstream_provenance(df, source_name):
        """Keep provenance that upstream actually publishes; absent model metadata stays NULL."""
        columns = {c.upper(): c for c in df.columns}
        def value(name, dtype='string'):
            return F.col(columns[name]).cast(dtype) if name in columns else F.lit(None).cast(dtype)
        medication = source_name.endswith('.map_med_admin')
        reason = F.lit(None).cast('string')
        if medication:
            valid = F.coalesce(value('SNOMED_VALID_DRUG_DOMAIN_IND', 'boolean'), F.lit(False))
            reason = (F.when(value('SNOMED_VALID_DRUG_DOMAIN_IND', 'boolean') == F.lit(False), 'non_drug_domain')
                      .when(value('SNOMED_VALIDATED_CODE').isNull(), 'no_validated_code')
                      .when(~valid, 'drug_domain_not_validated'))
            # No row filter: mapping rejection must never remove an administration.
            df = (df.withColumn('SNOMED_CODE', F.when(valid, value('SNOMED_VALIDATED_CODE')))
                    .withColumn('SNOMED_STR', F.when(valid, value('SNOMED_VALIDATED_STR'))))
        return df.withColumn('SNOMED_PROVENANCE', F.struct(
            F.coalesce(value('SNOMED_TYPE'), value('SNOMED_VALIDATED_SOURCE'), value('SNOMED_SOURCE'),
                       F.lit('upstream_method_not_recorded')).alias('method'),
            value('SNOMED_SIMILARITY', 'double').alias('similarity'),
            F.coalesce(value('SNOMED_SOURCE'), F.lit(source_name)).alias('source'),
            value('SNOMED_MODEL').alias('model'), value('SNOMED_MODEL_VERSION').alias('model_version'),
            F.lit('pharos_validated_drug_v2' if medication else 'pharos_upstream_v2').alias('rule_id'),
            reason.alias('rejection_reason')))


    def mapping_fields(prefix):
        kinds = {'code': StringType(), 'term': StringType(), 'method': StringType(),
                 'similarity': DoubleType(), 'source': StringType(), 'model': StringType(),
                 'model_version': StringType(), 'rule_id': StringType(), 'rejection_reason': StringType(),
                 'vocabulary_release': StringType()}
        return [StructField(prefix + suffix, typ, True,
                {'comment': 'SNOMED mapping ' + suffix + '; provenance is retained for rejected mappings.'})
                for suffix, typ in kinds.items()]


    VALUESET_FIELDS = {
        'pharos_person': [('sex', 'sex'), ('ethnic_group', 'ethnic_group')],
        'pharos_family_cancer_history': [('relation_degree', 'relation_degree')],
        'pharos_imaging': [('imaging_type', 'imaging_type')],
        'pharos_followup': [('vital_status', 'vital_status')],
        'pharos_treatment': [('treatment_type', 'treatment_type')],
        'pharos_pathology': [('path_atypical', 'path_atypical'), ('path_benign', 'path_benign')],
        'pharos_sample': [('tissue_site', 'tissue_site')],
        'pharos_metastasis': [('metastasis_site', 'metastasis_site')],
    }


    def enriched_schema(schema, target):
        names = [f.name for f in schema]
        prefixes = [n[:-4] for n in names if n.endswith('snomed_code')]
        for column, category in VALUESET_FIELDS.get(target, []):
            # Some pre-existing placeholders do not yet have a source; do not invent one.
            if column in names and column + '_snomed_' not in prefixes:
                prefixes.append(column + '_snomed_')
        schema = StructType([f for f in schema if not any(f.name.startswith(p) for p in prefixes)])
        for prefix in prefixes:
            for field in mapping_fields(prefix): schema.add(field)
        return schema


    def build_snomed_lookup():
        rows = [(*row, EXPECTED_CODES[(row[0], row[1])]) for row in LOOKUP_ROWS]
        labels = spark.createDataFrame(rows, 'category string, source_label string, concept_code string, maps_to_null boolean, ancestor_code string, allowed_domains string, rejection_reason string, expected_concept_code string')
        concepts = spark.table('4_prod.omop.concept').filter(F.col('vocabulary_id') == 'SNOMED')
        vocabulary = spark.table('4_prod.omop.vocabulary').filter(F.col('vocabulary_id') == 'SNOMED').first()
        release = vocabulary.vocabulary_version
        lookup = labels.alias('l').join(concepts.alias('c'), F.col('l.concept_code') == F.col('c.concept_code'), 'left').select(
            'l.*', F.col('c.concept_name').alias('concept_term'), 'c.concept_id', 'c.domain_id', 'c.standard_concept', 'c.invalid_reason')
        assert not lookup.groupBy('category','source_label').count().filter('count != 1').take(1), 'Tier 1: duplicate lookup label'
        violations = lookup.filter((~F.col('maps_to_null') & F.col('concept_id').isNull()) |
            F.col('invalid_reason').isNotNull() | (F.col('maps_to_null') & F.col('concept_code').isNotNull()))
        assert not violations.take(1), 'Tier 1 unresolved/invalid mapping: ' + str(violations.collect())
        # Standardness is recorded, not required: sex and ethnicity are valid nonstandard codes.
        violations = lookup.filter(~F.col('maps_to_null') & ~F.array_contains(F.split('allowed_domains', r'\|'), F.col('domain_id')))
        assert not violations.take(1), 'Tier 2 domain: ' + str(violations.collect())
        ancestors = (concepts.select(F.col('concept_code').alias('ancestor_code'), F.col('concept_id').alias('ancestor_id')))
        expected = lookup.filter(~F.col('maps_to_null') & F.col('ancestor_code').isNotNull()).join(ancestors, 'ancestor_code', 'left')
        hierarchy = spark.table('4_prod.omop.concept_ancestor')
        violations = expected.join(hierarchy, (expected.ancestor_id == hierarchy.ancestor_concept_id) &
                                  (expected.concept_id == hierarchy.descendant_concept_id), 'left_anti')
        assert not violations.take(1), 'Tier 2 hierarchy: ' + str(violations.collect())
        violations = lookup.filter(~F.col('concept_code').eqNullSafe(F.col('expected_concept_code')))
        assert not violations.take(1), 'Tier 3 semantic pin: ' + str(violations.collect())
        relationships = spark.table('4_prod.omop.concept_relationship').filter(
            (F.col('relationship_id') == 'Maps to') & F.col('invalid_reason').isNull())
        standard = spark.table('4_prod.omop.concept').filter((F.col('standard_concept') == 'S') & F.col('invalid_reason').isNull())
        standard_ids = (relationships.join(standard, relationships.concept_id_2 == standard.concept_id, 'inner')
            .groupBy('concept_id_1').agg(F.sort_array(F.collect_set('concept_id_2')).alias('standard_concept_ids')))
        lookup = (lookup.join(standard_ids, lookup.concept_id == standard_ids.concept_id_1, 'left').drop('concept_id_1')
            .withColumn('mapping_method', F.lit('manual_lookup_v2'))
            .withColumn('similarity', F.lit(None).cast('double'))
            .withColumn('vocabulary_release', F.lit(release)).withColumn('loaded_at', F.current_timestamp()))
        lookup.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(get_target_table('pharos_snomed_lookup'))
        return release


    def join_valueset(df, column, category):
        prefix = column + '_snomed_'
        lookup = spark.table(get_target_table('pharos_snomed_lookup')).filter(F.col('category') == category)
        missing = (df.select(F.col(column).alias('source_label')).filter('source_label IS NOT NULL').distinct()
                   .join(lookup.select('source_label'), 'source_label', 'left_anti'))
        assert not missing.take(1), f'Tier 1 uncovered labels for {column}: ' + str(missing.collect())
        lookup = lookup.select(F.col('source_label').alias('_lookup_label'), F.col('concept_code').alias(prefix+'code'),
            F.col('concept_term').alias(prefix+'term'), F.struct(
                F.col('mapping_method').alias('method'), 'similarity', F.lit('pharos_snomed_lookup').alias('source'),
                F.lit(None).cast('string').alias('model'), F.lit(None).cast('string').alias('model_version'),
                F.concat(F.lit(category+':'), F.col('source_label')).alias('rule_id'), 'rejection_reason').alias(prefix+'provenance'))
        return df.join(F.broadcast(lookup), df[column] == lookup._lookup_label, 'left').drop('_lookup_label')


    def complete_mappings(df, schema, target):
        for column, category in VALUESET_FIELDS.get(target, []):
            if column in df.columns:
                df = join_valueset(df, column, category)
        prefixes = [f.name[:-4] for f in schema if f.name.endswith('snomed_code')]
        for prefix in prefixes:
            assert prefix+'code' in df.columns, f'{target}: missing mapping projection {prefix}code'
            assert prefix+'provenance' in df.columns, f'{target}: missing mapping provenance {prefix}'
            provenance = F.col(prefix+'provenance')
            for name in ['method','similarity','source','model','model_version','rule_id','rejection_reason']:
                df = df.withColumn(prefix+name, provenance.getField(name))
            df = (df.withColumn(prefix+'code', F.col(prefix+'code').cast('string'))
                  .withColumn(prefix+'vocabulary_release', F.lit(VOCABULARY_RELEASE))
                  .withColumn(prefix+'rejection_reason', F.coalesce(F.col(prefix+'rejection_reason'),
                      F.when(F.col(prefix+'code').isNull(), F.lit('no_source_mapping')))))
            if prefix == 'menopausal_snomed_':
                df = df.withColumn(prefix+'rejection_reason', F.when(F.col('menopausal_status') == 'G04 - Not applicable', 'not_applicable').otherwise(F.col(prefix+'rejection_reason')))
            # Resolve every published term from the installed vocabulary, including upstream mappings.
            vocab = (spark.table('4_prod.omop.concept').filter(F.col('vocabulary_id') == 'SNOMED')
                .select(F.col('concept_code').alias('_vcode'), F.col('concept_name').alias('_vterm'),
                        F.col('invalid_reason').alias('_vinvalid'), F.col('concept_id').alias('_vid')))
            df = df.join(vocab, df[prefix+'code'] == vocab._vcode, 'left')
            rejected = F.col(prefix+'code').isNotNull() & (F.col('_vid').isNull() | F.col('_vinvalid').isNotNull())
            df = (df.withColumn(prefix+'rejection_reason', F.when(rejected, 'invalid_or_unresolved_vocabulary_code').otherwise(F.col(prefix+'rejection_reason')))
                .withColumn(prefix+'code', F.when(~rejected, F.col(prefix+'code')))
                .withColumn(prefix+'term', F.when(~rejected, F.col('_vterm')))
                .drop('_vcode','_vterm','_vinvalid','_vid',prefix+'provenance'))
        return df


    def severity_mapping(value, date, *fields):
        """Preserve severity-max, keeping code and provenance from the same deterministic row.

        A populated status wins first; severity precedes date. Null dates sort last
        for the winning max. Case preference is explicit, then the full payload.
        """
        payload = F.struct(value, *fields)
        return F.max(F.struct(F.col(value).isNotNull().alias('has_value'),
            F.coalesce(F.col(value), F.lit('')).alias('severity'),
            F.col(date).isNotNull().alias('has_date'), F.col(date).alias('date'),
            F.col(fields[0]).isNotNull().alias('has_mapping'),
            (F.col(value) == F.lower(F.col(value))).alias('lowercase_preferred'),
            F.to_json(payload).alias('stable_tie'), payload.alias('payload'))).getField('payload')

    VOCABULARY_RELEASE = build_snomed_lookup()
    sex_lookup = spark.createDataFrame(DEMOGRAPHIC_SEX_ROWS, "gender_cd long, sex string")
    ethnicity_lookup = spark.createDataFrame(DEMOGRAPHIC_ETHNICITY_ROWS, "ethnicity_cd long, ethnicity string, ethnic_group string")
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 7, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_person_comment = """
    The table contains demographic information about patients, including identifiers
    such as person ID, gender, birth year, and ethnicity.
    """

    schema_pharos_person = StructType([
        StructField(
            name="person_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            name="pharosid",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clinical record PharosID."}
        ),
        StructField(
            name="tumour_group",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Primary tumour group: Breast, Lung, Pancreas."}
        ),
        StructField(
            name="site",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Center identifier: QMUL, KCL, Public data."}
        ),
        StructField(
            name="yob",
            dataType=IntegerType(),
            nullable=True,
            metadata={"comment": "Year of birth. Dates not shared outside each site."}
        ),
        StructField(
            name="sex",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Sex at birth."}
        ),
        StructField(
            name="sex_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code mapped to sex category."}
        ),
        StructField(
            name="sex_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term mapped to the sex category."}
        ),
        StructField(
            name="ethnicity",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Ethnicity category (broad group)."}
        ),
        StructField(
            name="ethnic_group",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Ethnicity category (granular group)."}
        ),
        StructField(
            name="ethnic_group_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code mapped to ethnicity category."}
        ),
        StructField(
            name="ethnic_group_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term mapped to the ethnicity category."}
        ),
        StructField(
            name="ADC_UPDT",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking."}
        ),
        StructField(
            name="date_checked",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Date the relevant medical record was last checked."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_person = enriched_schema(schema_pharos_person, 'pharos_person')


    def create_pharos_person_incr():
        begin_build(get_target_table('pharos_person'), ['4_prod.bronze.map_person', '4_prod.bronze.map_diagnosis'])


        map_person = source_table(
            "4_prod.bronze.map_person"
        )

        processed_person = (
            map_person.drop("sex", "ethnicity", "ethnic_group")
            .join(sex_lookup, "gender_cd", "left")
            .join(ethnicity_lookup, "ethnicity_cd", "left")
            .fillna({"sex": "G09 Unknown", "ethnicity": "G09 - Unknown", "ethnic_group": "G09 Unknown"})
        )

        map_diagnosis = (
            source_table("4_prod.bronze.map_diagnosis")
            .select(
                "PERSON_ID",
                "OMOP_CONCEPT_ID",
                "ICD10_CODE"
            )
        )

        # OMOP concept IDs for breast cancer subtypes
        brc_add_ids = [
            45768522,  # Triple-negative breast cancer
            35624616,  # Germline BRCA-mutated, HER2-negative metastatic breast cancer
            602331     # Metastatic malignant neoplasm to left breast
        ]

        site = (
            map_diagnosis
            .withColumn(
                "tumour_group",
                F.when(
                    F.col("ICD10_CODE").like("C50%")
                    | F.col("OMOP_CONCEPT_ID").isin(brc_add_ids),
                    F.lit("001 Breast")
                )
            )
            .filter(F.col("tumour_group").isNotNull())
            .dropDuplicates(["PERSON_ID"])
        )

        final_df = (
            processed_person.alias("p")
            .join(
                site.alias("s"),
                F.col("p.person_id") == F.col("s.PERSON_ID"),
                "inner"
            )
            .select(
                F.col("p.person_id").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.col("s.tumour_group").cast(StringType()).alias("tumour_group"),
                F.lit(None).cast(StringType()).alias("site"),
                F.col("p.birth_year").cast(IntegerType()).alias("yob"),
                F.col("p.sex").cast(StringType()).alias("sex"),
                F.col("p.ethnicity").cast(StringType()).alias("ethnicity"),
                F.col("p.ethnic_group").cast(StringType()).alias("ethnic_group"),
                F.col("p.ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
            .dropDuplicates(["person_id"])
        )

        return complete_mappings(final_df, schema_pharos_person, 'pharos_person')


    if not SELECTED_TABLES or 'pharos_person' in SELECTED_TABLES:
        updates_df = create_pharos_person_incr()

        update_table(updates_df,get_target_table("pharos_person"),"person_id",schema_pharos_person,pharos_person_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 8, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_cohort_comment = "The table contains demographic information about patients, including identifiers such as person ID, gender, birth year, and ethnicity, etc."

    schema_pharos_cohort = StructType([
        StructField(
            name="person_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            name="pharosid",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ),    
        StructField(
            name="cohort",
            dataType=StringType(),
            nullable=True,
            metadata={
                "comment": "Pharos cohort groups."}
        )
    ])

    schema_pharos_cohort = enriched_schema(schema_pharos_cohort, 'pharos_cohort')

    def create_pharos_cohort_incr():
        begin_build(get_target_table('pharos_cohort'), ['4_prod.bronze.map_diagnosis'])


        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (
                    F.col("ICD10_CODE").like("C50%") |
                    F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
                )
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        final_df = (
            breast_cancer_cohort
            .select(
                F.col("PERSON_ID")
                    .cast(LongType())
                    .alias("person_id"),
                F.lit(None)
                    .cast(StringType())
                    .alias("pharosid"),
                F.lit(None)
                    .cast(LongType())
                    .alias("clinical_record_id"),
                F.lit(None)
                    .cast(StringType())
                    .alias("cohort"),
                F.current_timestamp()
                    .cast(StringType())
                    .alias("date_checked_met_sites"),
                F.col("_src_adc_updt")
                    .alias("ADC_UPDT")
            )
        )

        return complete_mappings(final_df, schema_pharos_cohort, 'pharos_cohort')


    if not SELECTED_TABLES or 'pharos_cohort' in SELECTED_TABLES:
        updates_df = create_pharos_cohort_incr()

        update_table(updates_df,get_target_table("pharos_cohort"),["person_id", "cohort"],schema_pharos_cohort,pharos_cohort_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 9, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    tumour_group_comment = (
        "This table records the tumour group associated with each Pharos participant. It links the participant to their primary tumour group, such as Breast, Pancreatic, or Lung, based on clinical diagnosis records."
    )


    schema_tumour_group = StructType([
        StructField(
            "tumour_group_id",
            LongType(),
            True,
            {"comment": "Surrogate primary key for each tumour group record."}
        ),
        StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "FK to person."}
        ),
        StructField(
            "clinical_record_id",
            LongType(),
            True,
            {"comment": "Unique clinical record PharosID."}
        ),
        StructField(
            "tumour_group",
            StringType(),
            True,
            {"comment": "Tumour group, e.g. Breast, Pancreatic, Lung."}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            True,
            {"comment": "Last update timestamp."}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            True,
            {"comment": "Last update timestamp."}
        ),
        StructField(
            "created_at",
            TimestampType(),
            True,
            {"comment": "Row creation timestamp."}
        ),
        StructField(
            "updated_at",
            TimestampType(),
            True,
            {"comment": "Row last-updated timestamp."}
        )
    ])

    schema_tumour_group = enriched_schema(schema_tumour_group, 'pharos_tumour_group')


    def create_tumour_group_incr():
        begin_build(get_target_table('pharos_tumour_group'), ['4_prod.bronze.map_diagnosis'])


        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")

        tumour_group_cohort = (
            map_diagnosis
        
            .filter(F.col("PERSON_ID").isNotNull())
            .withColumn(
                "tumour_group",
                F.when(
                    (F.col("ICD10_CODE").like("C50%")) |
                    (F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)),
                    "Breast"
                )
                .when(
                    F.col("ICD10_CODE").like("C25%"),
                    "Pancreatic"
                )
                .when(
                    F.col("ICD10_CODE").like("C33%") |
                    F.col("ICD10_CODE").like("C34%"),
                    "Lung"
                )
            )
            .filter(F.col("tumour_group").isNotNull())
            .groupBy("PERSON_ID", "tumour_group")
            .agg(
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        final_df = (
            tumour_group_cohort
            .select(
                F.lit(None).cast(LongType()).alias("tumour_group_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.col("tumour_group"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_tumour_group, 'pharos_tumour_group')


    if not SELECTED_TABLES or 'pharos_tumour_group' in SELECTED_TABLES:
        updates_df = create_tumour_group_incr()

        update_table(updates_df,get_target_table("pharos_tumour_group"),["person_id"],schema_tumour_group,tumour_group_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 10, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_medical_history_comment = "The table contains the medical history of partcipants."
    schema_pharos_medical_history = StructType([
        StructField("drug_snomed_code", StringType(), True),
        StructField("drug_snomed_term", StringType(), True),
        StructField(
            name="person_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant (TBC)."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ), 
        StructField(
            name="familyhistory_bca",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Family history of breast cancer - assessed at time of first diagnosis."}
        ),
        StructField(
            name="familyhistory_ovarian",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Family history of breast cancer - assessed at time of first diagnosis."}
        ),

        StructField(
            name="familyhistory_cancer",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Family history of any cancer - assessed at time of first diagnosis."}
        ),
        StructField(
            name="genetic_testing",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Whether patient underwent genetic testing."}
        ),
        StructField(
            name="any_germline_mutation",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "The presence of any pathological mutation found through genetic testing Mandatory only where genetic_testing =Yes."}
        ),
        StructField(
            name="brca1",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from BRCA1 testing."}
        ),
        StructField(
            name="brca2",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from BRCA2 testing."}
        ),
        StructField(
            name="tp53",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from TP53 testing."}
        ),
        StructField(
            name="palb2",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from PALB2 testing."}
        ),
        StructField(
            name="chek2",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from CHEK2 testing."}
        ),
        StructField(
            name="atm",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from ATM testing."}
        ),
        StructField(
            name="rad51c",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from RAD51C testing."}
        ),
        StructField(
            name="rad51d",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Results from RAD51D testing."}
        ),
        StructField(
            name="genetic_testing_details",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Record any information on the details of the genetic mutations reported."}
        ),
        StructField(
            name="menopausal_status",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Recorded menopausal status at time of first diagnosis."}
        ),
        StructField(
            name="menopausal_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code corresponding to the recorded menopausal status."}
        ),
        StructField(
            name="menopausal_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term corresponding to the recorded menopausal status."}
        ),
        StructField(
            name="menopause_age",
            dataType=IntegerType(),
            nullable=True,
            metadata={"comment": "Age in years at menopause - leave blank if unknown."}
        ),
        StructField(
            name="inferred_menopausal_status",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Inferred menopausal status based on age and treatment patterns:Postmenopausal if menstrual period had stopped naturally or surgically by bilateral oopherectomy. Those with unknown menopausal age, who reported irregular menses, hysterectomy, or MHT use, considered postmenopausal at age 53.Those taking aromatase inhibitors considered postmenopausal."}
        ),
        StructField(
            name="hrt",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "If patient is currently taking HRT or has in the past  - assessed at time of first diagnosis."}
        ),
        StructField(
            name="hrt_years",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Time in years of HRT use."}
        ),
        StructField(
            name="contraception_use",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Use of contraception."}
        ),
        StructField(
            name="contraception_details",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Details of contraception."}
        ),
        StructField(
            name="presentation",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Method of presentation to oncology."}
        ),
        StructField(
            name="height_diagnosis",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Height at primary cancer diagnosis (cm)."}
        ),
        StructField(
            name="weight_diagnosis",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Weight at primary cancer diagnosis (kg)."}
        ),
        StructField(
            name="bmi_diagnosis",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "BMI at primary cancer diagnosis - closest availabile BMI to diagnosis date within 6 months, with priority given to BMI prior to diagnosis."}
        ),
        StructField(
            name="smoking",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Smoking status at time of diagnosis."}
        ),
        StructField(
            name="smoking_no",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Number of cigarettes per day. If a range is given e.g. 5-10 per day, then the highest number is recorded."}
        ),
        StructField(
            name="smoking_years",
            dataType=IntegerType(),
            nullable=True,
            metadata={"comment": "Number of years smoked."}
        ),
        StructField(
            name="smoke_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code corresponding to the smoking status."}
        ),
        StructField(
            name="smoke_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term corresponding to the smoking status."}
        ),
        StructField(
            name="vape",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "E-cigarettes used at time of diagnosis."}
        ),
        StructField(
            name="vape_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code corresponding to the vaping status."}
        ),
        StructField(
            name="vape_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term corresponding to the vaping status."}
        ),
        StructField(
            name="alcohol",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Alcohol use at time of diagnosis."}
        ),
        StructField(
            name="alcohol_no",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Units per week.  If a range is given, then the highest number is recorded."}
        ),
        StructField(
            name="alcohol_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code corresponding to the alcohol use."}
        ),
        StructField(
            name="alcohol_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term corresponding to the alcohol use."}
        ),
        StructField(
            name="drug",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Recreational drugs used."}
        ),
        StructField(
            name="drug_details",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Recreational drugs details."}
        ),
        StructField(
            name="performance_diagnosis",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Performance status (ECOG) closest to diagnosis, within 30 days."}
        ),
        StructField(
            name="parous",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Patient has had pregnancies carried to viable gestational age."}
        ),
        StructField(
            name="gravidity_no",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Total number of pregnancies, regardless of outcome."}
        ),
        StructField(
            name="parity_no",
            dataType=FloatType(),
            nullable=True,
            metadata={"comment": "Number of live births."}
        ),
        StructField(
            name="age_first_pregnancy",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Age of patient (years) at first pregnancy."}
        ),
        StructField(
            name="breastfeed",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "If any children were breastfed."}
        ),
        StructField(
            name="pabc",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Pregnancy associated breast cancer, defined as breast cancer diagnosed during pregnancy, or within 12 months after giving birth."}
        ),
        StructField(
            name="time_pregnancytobc",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Time calculated between last live birth and breast cancer diagnosis (in years)"}
        ),
        StructField(
            name="ADC_UPDT",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            name="date_checked",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Timestamp of the update"}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_medical_history = enriched_schema(schema_pharos_medical_history, 'pharos_medical_history')

    def create_medical_history_incr():
        begin_build(get_target_table('pharos_medical_history'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_problem', '4_prod.bronze.map_numeric_events', '4_prod.bronze.map_family_history', '4_prod.bronze.map_mat_birth', get_target_table('pharos_person')])


        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")
        map_problem = source_table("4_prod.bronze.map_problem")
        map_numeric_events = source_table("4_prod.bronze.map_numeric_events")
        map_family_history = source_table("4_prod.bronze.map_family_history")
        map_birth = source_table("4_prod.bronze.map_mat_birth")
        pharos_person_sex = (
            source_table(get_target_table("pharos_person"))
            .select("person_id","sex"))

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (
                    F.col("ICD10_CODE").like("C50%") |
                    F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
                )
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        # Narrow down the cohort to only those who have a diagnosis of breast cancer for the data range of interest

        diagnosis = map_diagnosis.join(breast_cancer_cohort, ["PERSON_ID"], "inner")
        problem = map_problem.join(breast_cancer_cohort, "PERSON_ID", "inner")
        numeric_events = map_numeric_events.join(breast_cancer_cohort, "PERSON_ID", "inner")
        family_history = map_family_history.join(breast_cancer_cohort, "PERSON_ID", "semi").alias("f")
        birth = map_birth.join(breast_cancer_cohort, map_birth["MotherPerson_ID"] == breast_cancer_cohort["PERSON_ID"], "semi")

        comb_prob_diag = (
            problem
            .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "SNOMED_PROVENANCE", "SNOMED_TERM","ICD10_CODE", F.col("ONSET_DT_TM").alias("condition_date"))
            .unionByName(
                diagnosis
                .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "SNOMED_PROVENANCE", "SNOMED_TERM", "ICD10_CODE", F.col("DIAG_DT_TM").alias("condition_date")))
            .dropDuplicates()
        ).alias("c")

        #-------------FAMILY_CANCER_HISTORY

        # Captures Family History using OMOP concept IDs to supplement standard ICD-10 Z80 (Family history of primary malignant neoplasm) codes.
        family_history_add_ids = [
            4175994,   # Family history of malignant neoplasm of cervix uteri
            43530673,  # Family history of colorectal cancer
            4329111,   # Family history of breast cancer 2 gene mutation
            4160695,   # Family history of breast cancer 1 gene mutation
            4195970,   # Family history of cancer of colon
            37117109,  # Family history of malignant neoplasm of ovary in first degree relative
            42535500,  # Family history of breast cancer gene BRCA mutation
            3078338013,# Family history of breast cancer gene mutation in first degree relative
            45884753,  # Family history of colon cancer
            46273481,  # Family history of hereditary nonpolyposis colon cancer
            1243977,   # Family history of hereditary diffuse gastric cancer
            4326336,   # Family history of cancer of esophagus
            37311977,  # Family history of malignant neoplasm of urinary tract
            4176765,   # Family history of male breast cancer
            4334494,   # Family history of digestive organ cancer
            4179232,   # Family history of pancreatic cancer
            46273150,  # Family history of ureter cancer
            4328801,   # Family history of thyroid cancer
            4324202,   # Family history of liver cancer
            46273151,  # Family history of urethra cancer
            42535054,  # Family history of colon cancer over age 50
            4334339,   # Family history of thoracic cavity structure cancer
            4323762,   # Family history of brain cancer
            4322902,   # Family history of vagina cancer
            4177058,   # Family history of ileum cancer
            4328583,   # Family history of breast cancer in first degree relative <50
            35624517,  # Family history of breast cancer <50 in second degree female relative
            46274041,  # Family history of endometrium cancer
            4179082,   # Family history of eye cancer
            4324203,   # Family history of bone cancer
            764948,    # Family history of oral cavity cancer
            4327415    # Family history of testis cancer
        ]

        family_history = (
            comb_prob_diag
            .join(family_history, ["PERSON_ID"], "left")
            .withColumn(
                "familyhistory_bca_flag",
                F.when(
                    # ICD-10 Z803: Family history of malignant neoplasm of breast
                    (F.col("c.ICD10_CODE").like("Z803")) |
                    # OMOP concept_ids containing for family history breast cancer
                    (F.col("c.OMOP_CONCEPT_ID").isin(4179963, 4329111, 4160695, 42535500, 46270135, 4210263, 4176765, 4328583, 35624517, 46270155, 46270130)) |
                    (F.col("f.CONDITION_DESC") == "Breast cancer"),
                    "G01 - Yes")
                .when (
                    # Concept_id for "No FH: breast carcinoma": 4209112
                    (F.col("c.OMOP_CONCEPT_ID") == 4209112), "G00 - No")
            )
            .withColumn(
                "familyhistory_ovarian_flag",
                ## OMOP concept_ids containing for family history ovarian cancer
                F.when(F.col("c.OMOP_CONCEPT_ID").isin(4326681, 37117109, 37109210), "G01 - Yes")
                # Concept_id for No family history of ovarian cancer: 44804658
                .when(F.col("c.OMOP_CONCEPT_ID") == 44804658, "G00 - No")
            )
            .withColumn(
                "familyhistory_cancer_flag",
                F.when(
                    (F.col("c.OMOP_CONCEPT_ID").isin(family_history_add_ids)) |
                    (F.col("c.ICD10_CODE").like("Z80%")) |
                    (F.col("f.ICD10_CODE").rlike("^[CD]")) |
                    (F.col("familyhistory_bca_flag") == "G01 - Yes") |
                    (F.col("familyhistory_ovarian_flag") == "G01 - Yes"),
                    "G01 - Yes"
                ).otherwise("G09 - Unknown")
            )
            .groupBy("PERSON_ID")
            .agg(
                F.max("familyhistory_bca_flag").alias("familyhistory_bca"),
                F.max("familyhistory_ovarian_flag").alias("familyhistory_ovarian"),
                F.min("familyhistory_cancer_flag").alias("familyhistory_cancer")
                )
        )
            # Restrict lifestyle and biometric features to the peri-diagnostic period. # Window: [Diagnosis - 1 year] to [Diagnosis + 7 days].

        check_date_range = (
            breast_cancer_cohort
            .withColumn("check_onset_date", F.date_sub(F.col("brc_diag_date"), 365))
            .withColumn("check_offset_date", F.date_add(F.col("brc_diag_date"), 7))
            .select("PERSON_ID", "check_onset_date", "check_offset_date")
        )

        #-------------SMOKING, ALCOHOL & DRUGS
        # Get the range of dates to check for the personal lifestyles

        # List of Source Identifiers representing smoking status.
        never_smoker = [
            "397731016",   # Never smoked
            "397732011"    # Never smoked tobacco
        ]
        non_smoker = [
            "14866014",    # Non-smoker
            "169636015",   # Non-smoker for medical reasons
            "2157522017"  # Current non smoker but past smoking history unknown
        ]
        current_smoker = [
            "128130017",   # Smoker
            "503483019",   # Current smoker
            "4589211015",  # Smoking
            "2912728015",  # Smokes tobacco daily
            "108938018",   # Cigarette smoker
            "344798019",   # Light cigarette smoker
            "344801013",   # Moderate cigarette smoker
            "344802018",   # Heavy cigarette smoker
            "344803011",   # Very heavy cigarette smoker
            "344804017",   # Chain smoker
            "136515019",   # Pipe smoker
            "99639019",    # Cigar smoker
            "344797012",   # Occasional cigarette smoker
            "3308893017",  # Occasional tobacco smoker
            "2973829011"   # Water pipe smoker
        ]
        past_smoker = [
            "15047015",          # Ex-smoker
            "15046012",          # Former smoker
            "250373019",         # Stopped smoking
            "418914010",         # Ex-cigarette smoker
            "397737017",         # Ex-light cigarette smoker (1-9/day)
            "397738010",         # Ex-moderate cigarette smoker (10-19/day)
            "397739019",         # Ex-heavy cigarette smoker (20-39/day)
            "397740017",         # Ex-very heavy cigarette smoker (40+/day)
            "3513199018",        # Ex-smoker for less than 1 year
            "3047429015",        # Ex-smoker for more than 1 year
            "2735181000000113",  # Ex-smoker amount unknown
            "397744014",         # Ex-cigarette smoker amount unknown
            "250376010",         # Ex-pipe smoker
            "250377018",         # Ex-cigar smoker
            "F17.20"             # previousSmoker
        ]
        smoking_unknown_usage = [
            "172772016",   # Smoking AND/OR drinking habits
            "3084413016",  # Smoking/drinking/substance abuse habits
            "489484014",   # Tobacco smoking consumption - finding
            "106720017"    # Smoke (General/Unspecified)
        ]
        # List of Source Identifiers representing drinking status.
        alcoholic = [
            "1462019",    # Alcohol user
            "481158013",  # Drinks alcohol
            "Z72.1",      # Alcohol use
            "3012209018", # Admits alcohol use
            "2765571000000113", # History of alcohol use
            "1206561011", # Details of alcohol drinking behaviour
            "489467015",  # Alcohol intake - finding
            "2695747019", # Alcohol intake exceeds recommended limit
            "250338016",  # XS - Excessive alcohol consumption
            "342370018",  # Alcoholic binges exceeding sensible amounts
            "299061000000117", # Hazardous alcohol use
            "3322030015", # Unhealthy alcohol drinking behaviour
            "1216142012", # Alcohol drinking behaviour
            "478024019",  # Alcohol-induced epilepsy
            "2694326014", # Alcohol intake within daily limit
            "250340014",  # Alcohol intake within sensible limits
            "1154571000000110", # Alcohol misuse enhanced service completed
        ]

        non_alcoholic = [
            "291354017",  # 4022664 - Does not drink alcohol
            "2659854017", # 4022664 - Non - drinker alcohol
            "442138014",  # 4116983 - Abstinent alcoholic
            "250317010",  # 4052945 - Stopped drinking alcohol
            "339752015",  # 4022703 - Alcohol-free diet
        ]
        # List of Source Identifiers representing vaping status.
        current_vaper = [
            "5169833015", # Daily vaper
            "5169699016", # Daily vape user
            "3747608014", # Vaper
            "3773975015", # Vaper with nicotine
            "3769372019", # Nicotine-filled electronic cigarette vaper
            "3770507018", # Non-nicotine-filled electronic cigarette vaper
        ]

        vaping_usage_unknown = [
            "3747483013", # Vape
            "3850395015", # Vaping
        ]
        # List of Source Identifiers representing drug using status.

        recreational_drug_use = [
            "45692019",   # Recreational drug user
            "169644015",  # Occasional drug user
            "2548201014", # Episodic use of drugs
            "2159187014", # H/O: recreational drug use
            "2986925015", # History of recreational drug use
            "74848019",   # Ex-drug user
            "2548992019", # Previously injecting drug user (if no dependence specified)
        ]

        drug_abuse = [
            "450216018",  # Illicit drug use
            "2548235012", # Current drug user
            "11819014",   # Drug addiction
            "342438017",  # Drug addict
            "1216143019", # Drug misuse behaviour
            "342426014",  # Drug abuse behaviour
            "342461015",  # IVDU - Intravenous drug user
            "342465012",  # Intravenous drug user
            "339576014",  # Injecting drug user
            "339577017",  # Drug injector
            "342457014",  # Injects drugs intramuscularly
            "342455018",  # Injects drugs subcutaneously
            "427785011",  # Hypodermic drug injection
            "342443012",  # Smokes drugs
            "342448015",  # Intranasal drug use
            "342441014",  # Misuses drugs orally
            "342451010",  # Misuses drugs rectally
            "1210093012", # History of drug abuse
            "2476701016", # H/O: drug abuse
            "494054010",  # Ex-drug addict
            "494055011",  # Ex-drug misuser
            "3766542013", # Abuse of ecstasy type drug
            "342435019",  # Poly-drug misuser
            "295322016",  # Nondependent mixed drug abuse
            "253490011",  # Suspected abuse hard drugs
        ]

        smoke_cond = F.col("SOURCE_IDENTIFIER").isin(
            never_smoker +
            non_smoker +
            current_smoker +
            past_smoker +
            smoking_unknown_usage
        )

        vape_cond = (
            F.col("SOURCE_IDENTIFIER").isin(
                current_vaper + vaping_usage_unknown
            ) |
            (F.col("SOURCE_IDENTIFIER") == "5169692013")
        )

        alcohol_cond = (
            F.col("SOURCE_IDENTIFIER").isin(
                non_alcoholic + alcoholic
            ) |
            (F.col("SOURCE_IDENTIFIER") == "2988881016")
        )

        drug_cond = F.col("SOURCE_IDENTIFIER").isin(
            recreational_drug_use +
            drug_abuse
        )


        smoke_alcohol_drug = (
            comb_prob_diag
            .join(check_date_range, ["PERSON_ID"], "left")
            .filter(
                (F.col("condition_date").between(F.col("check_onset_date"), F.col("check_offset_date"))))

            #SMOKING ----------------
            .withColumn(
                "smoking",
                F.when(
                    F.col("SOURCE_IDENTIFIER").isin(never_smoker),
                    "000 - Never smoked"
                )
                .when(
                    F.col("SOURCE_IDENTIFIER").isin(non_smoker),
                    "001 - Non-smoker"
                )
                .when(
                    F.col("SOURCE_IDENTIFIER").isin(current_smoker),
                    "002 - Yes, current smoker"
                )
                .when(
                    F.col("SOURCE_IDENTIFIER").isin(past_smoker),
                    "003 - Yes, past smoker"
                )
                .when(
                    F.col("SOURCE_IDENTIFIER").isin(smoking_unknown_usage),
                    "004 - Yes, unknown usage"
                )
            )
            .withColumn("smoke_snomed_code", F.when(smoke_cond, F.col("SNOMED_CODE"))).withColumn('smoke_snomed_provenance', F.when(smoke_cond, F.col("SNOMED_PROVENANCE")))
            .withColumn("smoke_snomed_term", F.when(smoke_cond, F.col("SNOMED_TERM")))

            # VAPING ----------------
            .withColumn(
                "vape",
                F.when(F.col("SOURCE_IDENTIFIER").isin(current_vaper),
                        "002 - Yes, current smoker"
                )
                .when(F.col("SOURCE_IDENTIFIER") == "5169692013", "003 - Yes, past smoker")
                .when(F.col("SOURCE_IDENTIFIER").isin(vaping_usage_unknown),
                        "004 - Yes, unknown usage"
                )
            )
            .withColumn("vape_snomed_code", F.when(vape_cond, F.col("SNOMED_CODE"))).withColumn('vape_snomed_provenance', F.when(vape_cond, F.col("SNOMED_PROVENANCE")))
            .withColumn("vape_snomed_term", F.when(vape_cond, F.col("SNOMED_TERM")))

            # ALCOHOL ----------------
            .withColumn(
                "alcohol",
                F.when(
                    F.col("SOURCE_IDENTIFIER").isin(non_alcoholic),
                    "000 - No alcohol use"
                )
                .when(
                    F.col("SOURCE_IDENTIFIER").isin(alcoholic),
                    "001 - Drinks alcohol"
                )
                .when(
                    # 2988881016 "Drinks alcohol daily"
                    F.col("SOURCE_IDENTIFIER") == "2988881016",
                    "002 - Drinks alcohol - Regularly"
                )
            )
            .withColumn("alcohol_snomed_code", F.when(alcohol_cond, F.col("SNOMED_CODE"))).withColumn('alcohol_snomed_provenance', F.when(alcohol_cond, F.col("SNOMED_PROVENANCE")))
            .withColumn("alcohol_snomed_term", F.when(alcohol_cond, F.col("SNOMED_TERM")))

            # DRUGS ----------------
            .withColumn(
                "drug",
                F.when(F.col("SOURCE_IDENTIFIER").isin(recreational_drug_use),
                "001 Yes - Recreationally")
                .when(F.col("SOURCE_IDENTIFIER").isin(drug_abuse),
                "002 Yes - Abuses drugs")
            )
            .withColumn("drug_snomed_code", F.when(drug_cond, F.col("SNOMED_CODE"))).withColumn('drug_snomed_provenance', F.when(drug_cond, F.col("SNOMED_PROVENANCE")))
            .withColumn("drug_snomed_term", F.when(drug_cond, F.col("SNOMED_TERM")))
        )

        smoke_alcohol_drug = (
            smoke_alcohol_drug
            .groupBy("PERSON_ID")
            .agg(
                severity_mapping('smoking', "condition_date", 'smoke_snomed_code', 'smoke_snomed_provenance', 'smoke_snomed_term').alias("smoke"),

                severity_mapping('vape', "condition_date", 'vape_snomed_code', 'vape_snomed_provenance', 'vape_snomed_term').alias("vape_rec"),

                severity_mapping('alcohol', "condition_date", 'alcohol_snomed_code', 'alcohol_snomed_provenance', 'alcohol_snomed_term').alias("alcohol_rec"),

                severity_mapping('drug', "condition_date", 'drug_snomed_code', 'drug_snomed_provenance', 'drug_snomed_term').alias("drug_rec")
            )
            .select(
                "PERSON_ID",

                F.col("smoke.smoking").alias("smoking"),
                F.col("smoke.smoke_snomed_code").alias("smoke_snomed_code"), F.col("smoke.smoke_snomed_provenance").alias("smoke_snomed_provenance"),
                F.col("smoke.smoke_snomed_term").alias("smoke_snomed_term"),

                F.col("vape_rec.vape").alias("vape"),
                F.col("vape_rec.vape_snomed_code").alias("vape_snomed_code"), F.col("vape_rec.vape_snomed_provenance").alias("vape_snomed_provenance"),
                F.col("vape_rec.vape_snomed_term").alias("vape_snomed_term"),

                F.col("alcohol_rec.alcohol").alias("alcohol"),
                F.col("alcohol_rec.alcohol_snomed_code").alias("alcohol_snomed_code"), F.col("alcohol_rec.alcohol_snomed_provenance").alias("alcohol_snomed_provenance"),
                F.col("alcohol_rec.alcohol_snomed_term").alias("alcohol_snomed_term"),

                F.col("drug_rec.drug").alias("drug"),
                F.col("drug_rec.drug_snomed_code").alias("drug_snomed_code"), F.col("drug_rec.drug_snomed_provenance").alias("drug_snomed_provenance"),
                F.col("drug_rec.drug_snomed_term").alias("drug_snomed_term"),
            )
            .fillna({
                "smoking": "G09 Unknown",
                "vape": "G09 Unknown",
                "alcohol": "G09 Unknown",
                "drug": "G09 Unknown"
            })
        )
        # MENOPAUSE ----------------

        pre_menop = ["Before menopause", "Premenopausal state", "Excessive bleeding in the premenopausal period"]

        post_menop = [
            "Post-menopausal", "Postmenopausal state", "Postmenopausal", "Postmenopausal bleeding",
            "PMB - Postmenopausal bleeding", "Bleeding after menopause", "History of postmenopausal bleeding",
            "H/O: postmenopausal bleeding", "Postartificial menopausal syndrome", "Premature menopause",
            "Postmenopausal osteoporosis", "Postmenopausal osteoporosis; Multiple sites",
            "Postmenopausal osteoporosis with pathological fracture", "Postmenopausal atrophic vaginitis",
            "Post menopausal depression", "Postsurgical menopause", "Post-hysterectomy menopause",
            "States associated with artificial menopause", "History of natural age related menopause",
            "Postmenopausal hormone replacement therapy", "Postmenopausal urethral atrophy",
            "Postmenopausal endometrium", "Postmenopausal postcoital bleeding",
            "Endometrial cells, cytologically benign, in a postmenopausal woman"
        ]

        peri_menop = ["Peri-menopausal", "Perimenopausal", "Perimenopause", "Perimenopausal state",
                    "Menopausal and female climacteric states", "Abnormal perimenopausal bleeding",
                    "Perimenopausal disorder", "Perimenopausal atrophic vaginitis"]

        # Pre-compute lowered/trimmed lists for case-insensitive matching
        pre_menop_lower = [s.lower().strip() for s in pre_menop]
        post_menop_lower = [s.lower().strip() for s in post_menop]
        peri_menop_lower = [s.lower().strip() for s in peri_menop]

        menopause = (
            comb_prob_diag.alias("c")
            .join(
                breast_cancer_cohort.alias("b"),
                F.col("c.PERSON_ID") == F.col("b.PERSON_ID"),
                "inner"
            )
            .join(
                pharos_person_sex.alias("s"),
                F.col("s.person_id") == F.col("b.PERSON_ID"),
                "left"
            )
            .filter(F.col("condition_date") <= F.col("brc_diag_date"))
            .withColumn(
                "menopausal_status_flag",
                F.when(F.col("sex").isin("M", "002 - Male"), "G04 - Not applicable")
                .when(
                    F.lower(F.trim(F.col("SOURCE_STRING"))).isin(pre_menop_lower),
                    "001 - Pre-menopausal"
                )
                .when(
                    F.lower(F.trim(F.col("SOURCE_STRING"))).isin(peri_menop_lower),
                    "002 - Peri-menopausal"
                )
                .when(
                    F.lower(F.trim(F.col("SOURCE_STRING"))).isin(post_menop_lower),
                    "003 - Post-menopausal"
                )
            )
            .withColumn(
                "menopausal_snomed_code",
                F.when(
                    (F.col("menopausal_status_flag").isNotNull() & (F.col("menopausal_status_flag") != "G04 - Not applicable")),
                    F.col("SNOMED_CODE")
                )
            ).withColumn('menopausal_snomed_provenance', F.when(
                    (F.col("menopausal_status_flag").isNotNull() & (F.col("menopausal_status_flag") != "G04 - Not applicable")),
                    F.col("SNOMED_PROVENANCE")
                ))
            .withColumn(
                "menopausal_snomed_term",
                F.when(
                    (F.col("menopausal_status_flag").isNotNull() & (F.col("menopausal_status_flag") != "G04 - Not applicable")),
                    F.col("SNOMED_TERM")
                )
            )
            .dropDuplicates()
            .filter(F.col("menopausal_status_flag").isNotNull())
            .groupBy(F.col("b.PERSON_ID").alias("PERSON_ID"))
            .agg(
                severity_mapping('menopausal_status_flag', "condition_date", 'menopausal_snomed_code', 'menopausal_snomed_provenance', 'menopausal_snomed_term').alias("menopausal_status")
            )
            .select(
                "PERSON_ID",
                F.col("menopausal_status.menopausal_status_flag").alias("menopausal_status"),
                F.col("menopausal_status.menopausal_snomed_code").alias("menopausal_snomed_code"), F.col("menopausal_status.menopausal_snomed_provenance").alias("menopausal_snomed_provenance"),
                F.col("menopausal_status.menopausal_snomed_term").alias("menopausal_snomed_term")
            )
        )

        processed_numeric_events = (
            numeric_events
            .join(check_date_range, ["PERSON_ID"], "left")
            .filter(
                F.col("PERFORMED_DT_TM").between(F.col("check_onset_date"), F.col("check_offset_date"))
            )
            # Get the absolute time difference between the event and the earliest diagnosis date to help identify the most recent events.
            .withColumn(
                "abs_diff",
                F.abs(F.datediff(F.col("PERFORMED_DT_TM"),F.col("brc_diag_date")))
            )
        )

        # Get source codes for the smoking amount and aclcohol amount
        smoke_codes = [
            4127902,    # Number of Cigarettes Per Day Now
            71834925,   # Cigarettes Per Day at Booking
            71835447,   # Cigarettes Per Day at Delivery
            472635529,  # CCO cigarettes per day
            662214545,  # CCO cigarettes day
            999498499   # How many cigarettes do you usually smoke
        ]
        alcohol_codes = [
            71839205,   # Standard lifestyle measure of weekly consumption.
            71835023,   # Alcohol Units Pre Pregnancy Per Week
            71834759,   # Alcohol Units at Booking Per Week
            71835548,   # Alcohol Units at Delivery Per Week
            71844122    # (M) Alcohol Units Pre Pregnancy Per Week
            ]
    
        # Get the smoking and alcohol numbers for each patient if it exists
        window = Window.partitionBy("PERSON_ID", "lifestyle").orderBy(F.col("NUMERIC_RESULT").isNotNull().desc_nulls_last(), F.col("abs_diff").asc_nulls_last(), F.col("PERFORMED_DT_TM").desc_nulls_last(), F.col("NUMERIC_RESULT").desc_nulls_last())

        smoke_alcohol_no = (
            processed_numeric_events
            .withColumn("lifestyle",
                F.when(F.col("EVENT_CD").isin(smoke_codes), "smoking_no")
                .when(F.col("EVENT_CD").isin(alcohol_codes), "alcohol_no")
            )
            .filter(F.col("lifestyle").isNotNull())
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .groupBy("PERSON_ID")
            .pivot("lifestyle", ["smoking_no", "alcohol_no"])
            .agg(F.first("NUMERIC_RESULT"))
        )

        # HEIGHT, WEIGHT, BMI

        # Closest measurement per PERSON
        window = (
            Window
            .partitionBy("PERSON_ID", "OMOP_MANUAL_CONCEPT_NAME")
            .orderBy(F.col("NUMERIC_RESULT").isNotNull().desc_nulls_last(), F.col("abs_diff").asc_nulls_last(), F.col("PERFORMED_DT_TM").desc_nulls_last(), F.col("NUMERIC_RESULT").desc_nulls_last())
        )

        height_weight = (
            processed_numeric_events
            .filter(
                F.col("OMOP_MANUAL_CONCEPT_NAME").isin("Body height measure","Body weight measure")
            )
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .groupBy("PERSON_ID")
            .pivot("OMOP_MANUAL_CONCEPT_NAME",["Body height measure", "Body weight measure"])
            .agg(F.first("NUMERIC_RESULT"))
            .withColumn("height", F.col("Body height measure") / 100)
            .withColumnRenamed("Body weight measure", "weight")
            # Get bmi using height and weight
            .withColumn("bmi",F.col("weight") / (F.col("height") * F.col("height")))
            .select("PERSON_ID", "height", "weight", "bmi")
        )

        #PREGNANCY
    
        # Get the largest parity & gravidity number for each person
        window = Window.partitionBy("PERSON_ID", "EVENT_CD_DISPLAY").orderBy(F.col("NUMERIC_RESULT").isNotNull().desc_nulls_last(), F.col("NUMERIC_RESULT").desc_nulls_last(), F.col("PERFORMED_DT_TM").desc_nulls_last())
        parity_gravidity = (
            numeric_events
            .filter(
                (F.col("PERFORMED_DT_TM") < F.col("brc_diag_date")) &
                (F.col("EVENT_CD_DISPLAY").isin("Parity", "Gravida"))
            )
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .groupBy("PERSON_ID")
            .pivot("EVENT_CD_DISPLAY",["Parity", "Gravida"])
            .agg(F.first("NUMERIC_RESULT"))
        )

        processed_pregnancy = (
            parity_gravidity.alias("p")
            .join(
                pharos_person_sex.alias("s"),
                F.col("s.person_id") == F.col("p.PERSON_ID"),
                "left"
            )
            .withColumn("parous",
                        F.when((F.col("Parity") == 0), "G00 No")
                        .when((F.col("Parity") > 0), "G01 Yes")
                        .when(F.col("sex").isin("M", "002 - Male"), "G04 Not applicable")
                        )
            .select("p.PERSON_ID", "parous", F.col("Parity").alias("parity_no"), F.col("Gravida").alias("gravidity_no"))
            .dropDuplicates()
        )

        breastfeed_items = [
            "Partial Breastfeeding", "Exclusive Breastfeeding", "Breast and complementary feeds",
            "Partially breast milk feeding", "Exclusively breast milk feeding", "Other: BREASTFEEDING",
            "Other: MIX FEEDINJG", "Other: MIX FEEDIMG", "Other: Expressed breast milk",
            "Other: Mixed as per maternal request", "Other: Mixfeeding. Explained to always offer breast first and bottle afterwards.",
            "Other: MIXED", "Other: MIX  FEEDING", "Other: breastfeeding and topping up with EBM",
            "Other: EBM via bottle", "Other: ebm", "Other: mix feedings", "Other: mix feeeding",
            "Other: BF+bottle", "Other: MIX FEDING", "Other: Expressed Breast Milk", "Other: MIX FEEDIN",
            "Other: EBM + formula feeding", "Other: mixed", "Other: NMIX FEEDING", "Other: Mix feeding",
            "Other: Breastfeeds and tops up with formular milk.", "Other: B/F", "Other: bottle EBM",
            "Other: MX FEEDING", "Other: Mixed Feeding", "Other: MIX FEEDIG", "Other: Breast borderline IGUR",
            "Other: MIX FEEEDING", "Other: gGiving EBM from her finger", "Other: BREAST FEEDING PLUS EBM BY BOTTLE",
            "Other: mix feeing", "Other: MIX", "Other: MI X FEEDING", "Other: EBM and complementary feeds",
            "Other: expressed milk via bottle", "Other: Breast and formulae", "Other: EBM + artificial",
            "Other: Expressed breastmilk via bottle", "Other: N/MIX FEEDING", "Other: finger feeding",
            "Other: mixfeeding", "Other: MIX FEEDINBG", "Other: MMIX FEEDING", "Other: Mixed feeding",
            "Other: CUP FEEDING", "Other: MIX  FEDDING", "Other: breastfeeding and giving EBM 90mls 3hrly",
            "Other: EBM"
        ]
        breastfeed  = (
            birth.alias("bi")
            .join(
                pharos_person_sex.alias("s"),
                F.col("s.person_id") == F.col("bi.MotherPerson_ID"),
                "left"
            )
            .withColumn(
                "bf_flag",
                F.when(F.col("FeedingMethod").isin("Artificial", "No breast feeding at all", "atifical feeding",), "G00 No")
                .when(F.col("FeedingMethod").isin(breastfeed_items), "G01 Yes")
                .when(F.col("sex").isin("M", "002 - Male"), "G04 Not applicable")
            )
            .groupBy("MotherPerson_ID")
            .agg(F.max("bf_flag").alias("breastfeed"))
            .select(F.col("MotherPerson_ID").alias("PERSON_ID"), "breastfeed")
        )


        pregnancy = (
            processed_pregnancy
            .join(breastfeed, ["PERSON_ID"], "left")
            .fillna("G09 Unknown", ["parous","breastfeed"])
        )

        processed_df = (
            breast_cancer_cohort
            .join(family_history, ["PERSON_ID"], "left")
            .join(smoke_alcohol_drug, ["PERSON_ID"], "left")
            .join(smoke_alcohol_no, ["PERSON_ID"], "left")
            .join(height_weight, ["PERSON_ID"], "left")
            .join(menopause,["PERSON_ID"], "left")
            .join(pregnancy, ["PERSON_ID"], "left")
            .fillna("G09 Unknown", ["familyhistory_bca","familyhistory_ovarian","familyhistory_cancer","menopausal_status"])
            .drop("brc_diag_date")
            .dropDuplicates())

        final_df = (
            processed_df
            .select(
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.col("familyhistory_bca").cast(StringType()),
                F.col("familyhistory_ovarian").cast(StringType()),
                F.col("familyhistory_cancer").cast(StringType()),
                F.lit(None).cast(StringType()).alias("genetic_testing"),
                F.lit(None).cast(StringType()).alias("any_germline_mutation"),
                F.lit(None).cast(StringType()).alias("brca1"),
                F.lit(None).cast(StringType()).alias("brca2"),
                F.lit(None).cast(StringType()).alias("tp53"),
                F.lit(None).cast(StringType()).alias("palb2"),
                F.lit(None).cast(StringType()).alias("chek2"),
                F.lit(None).cast(StringType()).alias("atm"),
                F.lit(None).cast(StringType()).alias("rad51c"),
                F.lit(None).cast(StringType()).alias("rad51d"),
                F.lit(None).cast(StringType()).alias("genetic_testing_details"),
                F.col("menopausal_status").cast(StringType()),
                F.col("menopausal_snomed_code").cast(StringType()), F.col("menopausal_snomed_provenance"),
                F.col("menopausal_snomed_term").cast(StringType()),
                F.lit(None).cast(IntegerType()).alias("menopause_age"),
                F.lit(None).cast(StringType()).alias("inferred_menopausal_status"),
                F.lit(None).cast(StringType()).alias("hrt"),
                F.lit(None).cast(StringType()).alias("hrt_years"),
                F.lit(None).cast(StringType()).alias("contraception_use"),
                F.lit(None).cast(StringType()).alias("contraception_details"),
                F.lit(None).cast(StringType()).alias("presentation"),
                F.col("height").cast(FloatType()).alias("height_diagnosis"),
                F.col("weight").cast(FloatType()).alias("weight_diagnosis"),
                F.col("bmi").cast(FloatType()).alias("bmi_diagnosis"),
                F.col("smoking").cast(StringType()),
                F.col("smoking_no").cast(FloatType()),
                F.col("smoke_snomed_code").cast(StringType()), F.col("smoke_snomed_provenance"),
                F.col("smoke_snomed_term").cast(StringType()),
                F.lit(None).cast(IntegerType()).alias("smoking_years"),
                F.col("vape").cast(StringType()),
                F.col("vape_snomed_code").cast(StringType()), F.col("vape_snomed_provenance"),
                F.col("vape_snomed_term").cast(StringType()),
                F.col("alcohol").cast(StringType()),
                F.col("alcohol_no").cast(FloatType()),
                F.col("alcohol_snomed_code").cast(StringType()), F.col("alcohol_snomed_provenance"),
                F.col("alcohol_snomed_term").cast(StringType()),
                F.col("drug").cast(StringType()),
                F.col("drug_snomed_code").cast(StringType()), F.col("drug_snomed_provenance"),
                F.col("drug_snomed_term").cast(StringType()),
                F.lit(None).cast(StringType()).alias("drug_details"),
                F.lit(None).cast(StringType()).alias("performance_diagnosis"),
                F.col("parous").cast(StringType()),
                F.col("gravidity_no").cast(FloatType()),
                F.col("parity_no").cast(FloatType()),
                F.lit(None).cast(StringType()).alias("age_first_pregnancy"),
                F.col("breastfeed").cast(StringType()),
                F.lit(None).cast(StringType()).alias("pabc"),
                F.lit(None).cast(StringType()).alias("time_pregnancytobc"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        final_df = (
            final_df.join(pharos_person_sex.select(F.col("person_id"), F.col("sex").alias("_applicability_sex")), "person_id", "left")
            .withColumn("menopausal_status", F.when(F.col("_applicability_sex").isin("M", "002 - Male"), "G04 - Not applicable").otherwise(F.col("menopausal_status")))
            .withColumn("breastfeed", F.when(F.col("_applicability_sex").isin("M", "002 - Male"), "G04 - Not applicable").otherwise(F.col("breastfeed")))
            .drop("_applicability_sex")
        )
        return complete_mappings(final_df, schema_pharos_medical_history, 'pharos_medical_history')

    if not SELECTED_TABLES or 'pharos_medical_history' in SELECTED_TABLES:
        updates_df = create_medical_history_incr()


        # update_table(updates_df, get_target_table("pharos_medical_history"), ["person_id"], schema_pharos_medical_history, pharos_medical_history_comment)
        update_table(updates_df,get_target_table("pharos_medical_history"), ["person_id"], schema_pharos_medical_history, pharos_medical_history_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 11, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_fh_comment = "This table captures family history of cancer."

    schema_pharos_fh = StructType([
        StructField(
            name="family_history_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Surrogate PK."}
        ), 
        StructField(
            name="person_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),   
        StructField(
            name="pharosid",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "FK to person."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Unique clinical record PharosID."}
        ),
        StructField(
            name="cancer_type",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Type of cancer in family member."}
        ),
        StructField(
            name="cancer_type_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code for the type of family cancer history."}
        ),
        StructField(
            name="cancer_type_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term for the type of family cancer history."}
        ),
        StructField(
            name="relation_degree",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Highest degree relation."}
        ),
        StructField(
            name="relation_degree_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code mapped to relation_degree."}
        ),
        StructField(
            name="relation_degree_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term mapped to relation_degree."}
        ),
        StructField(
            name="relation_detail",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "Specific relation (e.g. Mother, Sister, Aunt)."}
        ),
        StructField(
            name="ADC_UPDT",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking."}
        ),
        StructField(
            name="date_checked",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Timestamp of the update"}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_fh = enriched_schema(schema_pharos_fh, 'pharos_family_cancer_history')

    # Family history is derived from two sources:
    # 1. The map_family_history table
    # 2. The map_problem and map_diagnosis tables using relevant family history codes (OMOP and ICD10)

    def create_fh_incr():
        begin_build(get_target_table('pharos_family_cancer_history'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_problem', '4_prod.bronze.map_family_history'])

        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")
        map_problem = source_table("4_prod.bronze.map_problem")
        map_family_history = source_table("4_prod.bronze.map_family_history")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (
                    F.col("ICD10_CODE").like("C50%") |
                    F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
                )
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        comb_prob_diag = (
        map_problem
        .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "SNOMED_PROVENANCE", "SNOMED_TERM", "ICD10_CODE", F.col("ONSET_DT_TM").alias("condition_date"))
        .unionByName(
            map_diagnosis
            .select("PERSON_ID", "SOURCE_STRING", "SOURCE_IDENTIFIER", "OMOP_CONCEPT_ID", "SNOMED_CODE", "SNOMED_PROVENANCE", "SNOMED_TERM", "ICD10_CODE", F.col("DIAG_DT_TM").alias("condition_date")))
        .dropDuplicates())

        # Get data from map_family_history

        cancer_type = (
            F.when(F.col("ICD10_CODE").rlike("^C74"), "001 - Adrenal cancer")
            .when(F.col("ICD10_CODE").rlike("^C21"), "002 - Anal cancer")
            .when(F.col("ICD10_CODE").rlike("^(C22\\.1|C24\\.0)"), "003 - Bile duct cancer")
            .when(F.col("ICD10_CODE").rlike("^C67"), "004 - Bladder cancer")
            .when(F.col("ICD10_CODE").rlike("^(C40|C41)"), "005 - Bone cancer")
            .when(F.col("ICD10_CODE").rlike("^C71"), "006 - Brain cancer")
            .when(F.col("ICD10_CODE").rlike("^C50"), "007 - Breast cancer")
            .when(F.col("ICD10_CODE").rlike("^C53"), "008 - Cervical cancer")
            .when(F.col("ICD10_CODE").rlike("^C18"), "010 - Colon cancer")
            .when(F.col("ICD10_CODE").rlike("^(C18|C19|C20)"), "011 - Colorectal cancer")
            .when(F.col("ICD10_CODE").rlike("^C54\\.1"), "012 - Endometrial cancer")
            .when(F.col("ICD10_CODE").rlike("^C69"), "013 - Eye cancer")
            .when(F.col("ICD10_CODE").rlike("^C57\\.0"), "014 - Fallopian tube cancer")
            .when(F.col("ICD10_CODE").rlike("^C23"), "015 - Gallbladder cancer")
            .when(F.col("ICD10_CODE").rlike("^C16"), "016 - Gastric cancer")
            .when(F.col("ICD10_CODE").rlike("^C64"), "019 - Kidney cancer")
            .when(F.col("ICD10_CODE").rlike("^C32"), "020 - Laryngeal cancer")
            .when(F.col("ICD10_CODE").rlike("^(C91|C92|C93|C94|C95)"), "021 - Leukaemia")
            .when(F.col("ICD10_CODE").rlike("^C22"), "022 - Liver cancer")
            .when(F.col("ICD10_CODE").rlike("^C34"), "023 - Lung cancer")
            .when(F.col("ICD10_CODE").rlike("^(C81|C82|C83|C84|C85|C86|C88)"), "024 - Lymphoma")
            .when(F.col("ICD10_CODE").rlike("^C38"), "025 - Mediastinal cancer")
            .when(F.col("ICD10_CODE").rlike("^C43"), "026 - Melanoma")
            .when(F.col("ICD10_CODE").rlike("^D46"), "027 - Myelodysplastic syndrome")
            .when(F.col("ICD10_CODE").rlike("^C90"), "028 - Myeloma")
            .when(F.col("ICD10_CODE").rlike("^(D45|D47\\.1|D47\\.3|D47\\.4)"), "029 - Myeloproliferative neoplasm")
            .when(F.col("ICD10_CODE").rlike("^C11"), "030 - Nasopharyngeal cancer")
            .when(F.col("ICD10_CODE").rlike("^C15"), "033 - Oesophageal cancer")
            .when(F.col("ICD10_CODE").rlike("^(C00|C01|C02|C03|C04|C05|C06)"), "034 - Oral cancer")
            .when(F.col("ICD10_CODE").rlike("^(C09|C10)"), "035 - Oropharyngeal cancer")
            .when(F.col("ICD10_CODE").rlike("^C56"), "036 - Ovarian cancer")
            .when(F.col("ICD10_CODE").rlike("^C25"), "037 - Pancreatic cancer")
            .when(F.col("ICD10_CODE").rlike("^C75\\.0"), "038 - Parathyroid cancer")
            .when(F.col("ICD10_CODE").rlike("^C60"), "039 - Penile cancer")
            .when(F.col("ICD10_CODE").rlike("^(C38\\.4|C45\\.0)"), "040 - Pleural cancer")
            .when(F.col("ICD10_CODE").rlike("^C48"), "041 - Primary peritoneal cancer")
            .when(F.col("ICD10_CODE").rlike("^C61"), "042 - Prostate cancer")
            .when(F.col("ICD10_CODE").rlike("^C20"), "043 - Rectal cancer")
            .when(F.col("ICD10_CODE").rlike("^(C07|C08)"), "044 - Salivary gland cancer")
            .when(F.col("ICD10_CODE").rlike("^(C30\\.0|C31)"), "045 - Sinonasal cancer")
            .when(F.col("ICD10_CODE").rlike("^(C43|C44)"), "046 - Skin cancer")
            .when(F.col("ICD10_CODE").rlike("^C17"), "047 - Small bowel cancer")
            .when(F.col("ICD10_CODE").rlike("^(C47|C49)"), "048 - Soft tissue sarcoma")
            .when(F.col("ICD10_CODE").rlike("^C72\\.0"), "049 - Spinal cord cancer")
            .when(F.col("ICD10_CODE").rlike("^C62"), "050 - Testicular cancer")
            .when(F.col("ICD10_CODE").rlike("^C37"), "051 - Thymus cancer")
            .when(F.col("ICD10_CODE").rlike("^C73"), "052 - Thyroid cancer")
            .when(F.col("ICD10_CODE").rlike("^(C65|C66)"), "053 - Upper urinary tract cancer")
            .when(F.col("ICD10_CODE").rlike("^C68\\.0"), "054 - Urethral cancer")
            .when(F.col("ICD10_CODE").rlike("^C52"), "055 - Vaginal cancer")
            .when(F.col("ICD10_CODE").rlike("^C51"), "056 - Vulvar cancer")
            .otherwise("G09 - Unknown")
        )
        family_history_1 = (
            map_family_history
            .filter(F.col("ICD10_CODE").rlike(r"^(C|D)"))
            .join(breast_cancer_cohort, "PERSON_ID", "left")
            .withColumn(
                "familyhistory_re",
                # 1st degree relatives: Mother, Father, Sister, Brother, Daughter, Son
                F.when(F.col("RELATION_CD").isin(81849783,81849776,81849760,81849757,81849790,81849795,153,160), 1)
                # 2nd degree relatives: Grandmother, Grandfather, Aunt, Uncle, Niece, Nephew
                .when(F.col("RELATION_CD").isin(81849796,81849770,81849784,81849774,81849785,81849791,81849786), 2)
                # 3rd degree relatives: Cousin
                .when(F.col("RELATION_CD") == 81849761, 3)
                # 7 None: Spouse, Partner, Wife, Husband, Step Parents, Foster Parents
                .when(F.col("RELATION_CD").isin(634771,81849794,81849793,81849762,81849777,81849797),7)
                # 9 Unknown: Not Specified, Null
                .otherwise(F.lit(9))
            )
            .withColumn(
                "relation_degree",
                F.when(F.col("familyhistory_re") == 1, "001 - 1st degree")
                .when(F.col("familyhistory_re") == 2, "002 - 2nd degree")
                .when(F.col("familyhistory_re") == 3, "003 - 3rd degree")
                .when(F.col("familyhistory_re") == 7, "000 - None")
                .otherwise("G09 - Unknown")
            )
            .withColumn("cancer_type", cancer_type)
            .withColumn(
                "cancer_type_snomed_code",
                F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_CODE")
                )
            ).withColumn('cancer_type_snomed_provenance', F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_PROVENANCE")
                ))
            .withColumn(
                "cancer_type_snomed_term",
                F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_TERM")
                )
            )
            .select("PERSON_ID", "relation_degree", F.col("RELATION_DESC").alias("relation_detail"),"cancer_type", "cancer_type_snomed_code", "cancer_type_snomed_provenance", "cancer_type_snomed_term","_src_adc_updt")
        )


        # OMOP concept mappings
        omop_to_category = {
            # 005 - Bone cancer
            4324203: "005 - Bone cancer",      # Family history of bone cancer

            # 006 - Brain cancer
            4323762: "006 - Brain cancer",     # Family history of brain cancer

            # 007 - Breast cancer
            42535500: "007 - Breast cancer",   # Family history of breast cancer gene BRCA mutation
            3078338013: "007 - Breast cancer", # Family history of breast cancer gene mutation in first degree relative
            4176765: "007 - Breast cancer",    # Family history of male breast cancer
            4328583: "007 - Breast cancer",    # Family history of breast cancer in first degree relative <50
            35624517: "007 - Breast cancer",   # Family history of breast cancer <50 in second degree female relative
            4179963: "007 - Breast cancer",    # Family history of breast cancer
            4329111: "007 - Breast cancer",    # Family history of breast cancer 2 gene mutation
            4160695: "007 - Breast cancer",    # Family history of breast cancer 1 gene mutation
            46270135: "007 - Breast cancer",   # Family history of breast cancer gene mutation in first degree relative
            46270155: "007 - Breast cancer",   # Family history of malignant neoplasm of breast diagnosed before 45 years of age
            46270130: "007 - Breast cancer",   # Family history of malignant neoplasm of breast at under age 50 in second degree female relative

            # 008 - Cervical cancer
            4175994: "008 - Cervical cancer",  # Family history of malignant neoplasm of cervix uteri

            # 010 - Colon cancer
            4195970: "010 - Colon cancer",     # Family history of cancer of colon
            45884753: "010 - Colon cancer",    # Family history of colon cancer
            42535054: "010 - Colon cancer",    # Family history of colon cancer over age 50

            # 011 - Colorectal cancer
            43530673: "011 - Colorectal cancer",   # Family history of colorectal cancer
            46273481: "011 - Colorectal cancer",   # Family history of hereditary nonpolyposis colon cancer

            # 012 - Endometrial cancer
            46274041: "012 - Endometrial cancer",  # Family history of endometrium cancer

            # 013 - Eye cancer
            4179082: "013 - Eye cancer",       # Family history of eye cancer

            # 016 - Gastric cancer
            1243977: "016 - Gastric cancer",   # Family history of hereditary diffuse gastric cancer

            # 022 - Liver cancer
            4324202: "022 - Liver cancer",     # Family history of liver cancer

            # 033 - Oesophageal cancer
            4326336: "033 - Oesophageal cancer",   # Family history of cancer of esophagus

            # 034 - Oral cancer
            764948: "034 - Oral cancer",       # Family history of oral cavity cancer

            # 036 - Ovarian cancer
            37117109: "036 - Ovarian cancer",  # Family history of malignant neoplasm of ovary in first degree relative
            4326681: "036 - Ovarian cancer",   # Family history of malignant neoplasm of ovary
            37109210: "036 - Ovarian cancer",  # Family history of malignant neoplasm of ovary in second degree relative

            # 037 - Pancreatic cancer
            4179232: "037 - Pancreatic cancer",    # Family history of pancreatic cancer

            # 042 - Prostate cancer
            4210263: "042 - Prostate cancer",  # Family history of malignant neoplasm of prostate

            # 047 - Small bowel cancer
            4177058: "047 - Small bowel cancer",   # Family history of ileum cancer

            # 050 - Testicular cancer
            4327415: "050 - Testicular cancer",    # Family history of testis cancer

            # 052 - Thyroid cancer
            4328801: "052 - Thyroid cancer",   # Family history of thyroid cancer

            # 053 - Upper urinary tract cancer
            37311977: "053 - Upper urinary tract cancer",   # Family history of malignant neoplasm of urinary tract
            46273150: "053 - Upper urinary tract cancer",   # Family history of ureter cancer

            # 054 - Urethral cancer
            46273151: "054 - Urethral cancer", # Family history of urethra cancer

            # 055 - Vaginal cancer
            4322902: "055 - Vaginal cancer",   # Family history of vagina cancer

            # G09 - Unknown
            4334494: "G09 - Unknown",          # Family history of digestive organ cancer
            4334339: "G09 - Unknown",          # Family history of thoracic cavity structure cancer
        }

        icd10_to_category = {
            "Z80.0": "G09 - Unknown",
            "Z80.1": "023 - Lung cancer",
            "Z80.2": "G09 - Unknown",
            "Z80.3": "007 - Breast cancer",
            "Z80.4": "036 - Ovarian cancer",
            "Z80.5": "053 - Upper urinary tract cancer",
            "Z80.6": "021 - Leukaemia",
            "Z80.7": "024 - Lymphoma",
            "Z80.8": "G09 - Unknown",
            "Z80.9": "G09 - Unknown",
        }
        # Convert Python dictionaries to Spark maps
        omop_map = F.create_map(
            *[F.lit(x) for kv in omop_to_category.items() for x in kv]
        )

        icd10_map = F.create_map(
            *[F.lit(x) for kv in icd10_to_category.items() for x in kv]
        )

        family_history_2 = (
            comb_prob_diag
            .join(breast_cancer_cohort, "PERSON_ID", "left")
            .withColumn(
                "cancer_type",
                F.coalesce(
                    omop_map[F.col("OMOP_CONCEPT_ID")],
                    icd10_map[F.col("ICD10_CODE")]
                )
            )
            .filter(F.col("cancer_type").isNotNull())
            .withColumn(
                "cancer_type_snomed_code",
                F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_CODE")
                )
            ).withColumn('cancer_type_snomed_provenance', F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_PROVENANCE")
                ))
            .withColumn(
                "cancer_type_snomed_term",
                F.when(
                    F.col("cancer_type").isNotNull(),
                    F.col("SNOMED_TERM")
                )
            )
            .withColumn(
                "relation_degree",
                F.when(F.col("OMOP_CONCEPT_ID").isin(3078338013, 46270135, 46270130, 4328583, 37117109), "001 - 1st degree")
                .when(F.col("OMOP_CONCEPT_ID").isin(35624517, 37109210), "002 - 2nd degree")
                .otherwise("G09 - Unknown")
            )
            .withColumn("relation_detail", F.lit(None))
            .select("PERSON_ID", "relation_degree", "relation_detail","cancer_type","cancer_type_snomed_code", "cancer_type_snomed_provenance","cancer_type_snomed_term","_src_adc_updt")
        )
        # Combine the two data source together
        combined_family_history = (
            family_history_1
            .unionByName(family_history_2)
            .dropDuplicates()
        )
        # Select the highest degree relation
        ranked_family_history = (
            combined_family_history
            .withColumn(
                "relation_rank",
                F.when(F.col("relation_degree").startswith("001"), 1)
                .when(F.col("relation_degree").startswith("002"), 2)
                .when(F.col("relation_degree").startswith("003"), 3)
                .when(F.col("relation_degree") == "G09 - Unknown", 4)
                .when(F.col("relation_degree") == "000 - None", 5)
                .otherwise(99)
            )
        )

        w = Window.partitionBy("PERSON_ID", "cancer_type") \
                .orderBy(F.col("relation_rank").between(1, 3).desc_nulls_last(), F.col("relation_rank").asc_nulls_last(), F.col("relation_detail").isNotNull().desc_nulls_last(), F.col("_src_adc_updt").desc_nulls_last(), (F.col("relation_detail") == F.lower(F.col("relation_detail"))).desc_nulls_last(), F.col("relation_detail").asc_nulls_last(), F.col("cancer_type_snomed_code").asc_nulls_last(), F.to_json(F.col("cancer_type_snomed_provenance")).asc_nulls_last())

        result = (
            ranked_family_history
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn", "relation_rank")
        )


        final_df = (
            result
            .select(
                F.lit(None).alias("family_history_id"),
                F.lit(None).alias("pharosid"),
                F.lit(None).alias("clinical_record_id"),
                F.col("PERSON_ID").alias("person_id"),
                F.col("cancer_type"),
                F.col("cancer_type_snomed_code"), F.col("cancer_type_snomed_provenance"),
                F.col("cancer_type_snomed_term"),
                F.col("relation_degree"),
                F.col("relation_detail"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
                F.current_timestamp().alias("date_checked"),            
                )
        )
        return complete_mappings(final_df, schema_pharos_fh, 'pharos_family_cancer_history')


    if not SELECTED_TABLES or 'pharos_family_cancer_history' in SELECTED_TABLES:
        updated_df = create_fh_incr()

        update_table(updated_df, get_target_table("pharos_family_cancer_history"), ["person_id", "cancer_type"], schema_pharos_fh, pharos_fh_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 12, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_tumour_comment = "Clinical characteristics at breast cancer diagnosis for participants in the PHAROS cohort."

    schema_pharos_tumour = StructType([
        StructField(
            "tumour_id",
            LongType(),
            True,
            {"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "Surrogate PK"}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ),
        StructField(
            "date_of_diagnosis",
            DateType(),
            True,
            {"comment": "Patient's initial cancer diagnosis date (biopsy or scan if metastatic)"}
        ),
        StructField(
            "year_of_diagnosis",
            IntegerType(),
            True,
            {"comment": "Patient's initial cancer diagnosis year"}
        ),
        StructField(
            "age_at_diagnosis",
            IntegerType(),
            True,
            {"comment": "Age at date of diagnosis"}
        ),
        StructField(
            "disease_status",
            StringType(),
            True,
            {"comment": "Type of disease: Primary, Recurrence, New Primary, Metastasis, High Risk, Asymmetry, Completion, Cosmetic, Atypia, Benign, Normal Control"}
        ),
        StructField(
            "laterality",
            StringType(),
            True,
            {"comment": "Laterality of the cancer. If bilateral, create a new row for each side"}
        ),
        StructField(
            "bilateral",
            BooleanType(),
            True,
            {"comment": "Indication of if cancer is bilateral or not"}
        ),
        StructField(
            "invasive_size_clinical",
            DoubleType(),
            True,
            {"comment": "Size of invasive tumour in mm from radiology. If multifocal, size of largest focus"}
        ),
        StructField(
            "total_size_clinical",
            DoubleType(),
            True,
            {"comment": "Total size of the tumour in mm from radiology, including in-situ disease"}
        ),
        StructField(
            "c_ajcc_edition",
            StringType(),
            True,
            {"comment": "TNM staging edition used"}
        ),
        StructField(
            "clinical_t_stage",
            StringType(),
            True,
            {"comment": "Clinical/pre-treatment T stage from TNM"}
        ),
        StructField(
            "clinical_n_stage",
            StringType(),
            True,
            {"comment": "Clinical/pre-treatment N stage from TNM"}
        ),
        StructField(
            "clinical_m_stage",
            StringType(),
            True,
            {"comment": "Clinical/pre-treatment M stage from TNM"}
        ),
        StructField(
            "clinical_stage",
            StringType(),
            True,
            {"comment": "Clinical/pre-treatment stage I-IV"}
        ),
        StructField(
            "diag_npi",
            DoubleType(),
            True,
            {"comment": "Nottingham Prognostics Index (NPI)"}
        ),
        StructField(
            "disease_inflammatory",
            BooleanType(),
            True,
            {"comment": "Inflammatory Breast cancer (If yes, T stage should be T4)"}
        ),
        StructField(
            "neoadjuvant_indication",
            StringType(),
            True,
            {"comment": "Whether or not patient had neo-adjuvant treatment"}
        ),
        StructField(
            "dx_test",
            StringType(),
            True,
            {"comment": "Oncotype DX Testing performed (Yes/No)"}
        ),
        StructField(
            "dx_node_status",
            StringType(),
            True,
            {"comment": "Oncotype DX Nodal Status"}
        ),
        StructField(
            "dx_er_score",
            DoubleType(),
            True,
            {"comment": "ER Gene Score as recorded on Oncotype DX results report"}
        ),
        StructField(
            "dx_pr_score",
            DoubleType(),
            True,
            {"comment": "PR Gene Score as recorded on Oncotype DX results"}
        ),
        StructField(
            "dx_her2_score",
            DoubleType(),
            True,
            {"comment": "HER2 Gene Score as recorded on Oncotype DX results"}
        ),
        StructField(
            "oncotype_dx_score",
            IntegerType(),
            True,
            {"comment": "Recurrence Score from Oncotype DX test"}
        ),
        StructField(
            "clin_response",
            StringType(),
            True,
            {"comment": "Response to treatment reported in EOT imaging"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_tumour = enriched_schema(schema_pharos_tumour, 'pharos_tumour')

    def create_pharos_tumour_incr():
        begin_build(get_target_table('pharos_tumour'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_person'])


        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        person = source_table("4_prod.bronze.map_person").select(F.col("person_id").alias("PERSON_ID"),"birth_year")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") |
                F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("date_of_diagnosis"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
            .withColumn("year_of_diagnosis", F.year(F.col("date_of_diagnosis")))
            .join(person, ["PERSON_ID"], "left")
            .withColumn("age_at_diagnosis", F.col("year_of_diagnosis") - F.col("birth_year"))
            .select("PERSON_ID", "date_of_diagnosis", "year_of_diagnosis", "age_at_diagnosis", "_src_adc_updt")
            .dropDuplicates()
        )

        final_df = (
            breast_cancer_cohort
            .select(
                F.lit(None).cast(LongType()).alias("tumour_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.col("date_of_diagnosis").cast(DateType()),
                F.col("year_of_diagnosis").cast(IntegerType()),
                F.col("age_at_diagnosis").cast(IntegerType()),
                F.lit(None).cast(StringType()).alias("disease_status"),
                F.lit(None).cast(StringType()).alias("laterality"),
                F.lit(None).cast(BooleanType()).alias("bilateral"),
                F.lit(None).cast(DoubleType()).alias("invasive_size_clinical"),
                F.lit(None).cast(DoubleType()).alias("total_size_clinical"),
                F.lit(None).cast(StringType()).alias("c_ajcc_edition"),
                F.lit(None).cast(StringType()).alias("clinical_t_stage"),
                F.lit(None).cast(StringType()).alias("clinical_n_stage"),
                F.lit(None).cast(StringType()).alias("clinical_m_stage"),
                F.lit(None).cast(StringType()).alias("clinical_stage"),
                F.lit(None).cast(DoubleType()).alias("diag_npi"),
                F.lit(None).cast(BooleanType()).alias("disease_inflammatory"),
                F.lit(None).cast(StringType()).alias("neoadjuvant_indication"),
                F.lit(None).cast(StringType()).alias("dx_test"),
                F.lit(None).cast(StringType()).alias("dx_node_status"),
                F.lit(None).cast(DoubleType()).alias("dx_er_score"),
                F.lit(None).cast(DoubleType()).alias("dx_pr_score"),
                F.lit(None).cast(DoubleType()).alias("dx_her2_score"),
                F.lit(None).cast(IntegerType()).alias("oncotype_dx_score"),
                F.lit(None).cast(StringType()).alias("clin_response"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )
        return complete_mappings(final_df, schema_pharos_tumour, 'pharos_tumour')


    if not SELECTED_TABLES or 'pharos_tumour' in SELECTED_TABLES:
        updated_df = create_pharos_tumour_incr()

        update_table(updated_df, get_target_table("pharos_tumour"), ["person_id", "tumour_id"], schema_pharos_tumour, pharos_tumour_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 13, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_imaging_comment = "This table contains longitudinal diagnostic imaging data for patients within the Pharos breast cancer cohort. It captures specific radiology metrics including lesion counts, breast density, ACR BI-RADS scores, and timing relative to the initial diagnosis date. Each row represents a unique imaging event per patient."

    schema_pharos_imaging = StructType([
        StructField(
            "imaging_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            name="pharosid",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "FK to person."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ),
        StructField(
            "image_date",
            DateType(),
            True,
            {"comment": "Imaging date"}
        ),
            StructField(
            "imaging_session_id",
            StringType(),
            True,
            {"comment": "The session ID for the imaging event"}
        ),
        StructField(
            "days_diagnosis_imaging",
            IntegerType(),
            True,
            {"comment": "Time in days between date diagnosis and date of imaging/radiology"}
        ),
        StructField(
            "imaging_type",
            StringType(),
            True,
            {"comment": "Type of diagnostic imaging"}
        ),
        StructField(
            "imaging_type_snomed_code",
            StringType(),
            True,
            {"comment": "SNOMED CT code for the diagnostic imaging"}
        ),
        StructField(
            "imaging_type_snomed_term",
            StringType(),
            True,
            {"comment": "SNOMED CT term for the diagnostic imaging"}
        ),
        StructField(
            "image_status",
            StringType(),
            True,
            {"comment": "Imaging rationale"}
        ),
        StructField(
            "image_site",
            StringType(),
            True,
            {"comment": "Imaging site. If multiple present, to be listed separated by a semi-colon (;)"}
        ),
        StructField(
            "image_contrast",
            StringType(),
            True,
            {"comment": "Contrast used"}
        ),
        StructField(
            "image_lesions",
            IntegerType(),
            True,
            {"comment": "Number of lesions"}
        ),
        StructField(
            "image_cal",
            StringType(),
            True,
            {"comment": "Calcification as recorded in the imaging report"}
        ),
        StructField(
            "breast_density",
            StringType(),
            True,
            {"comment": "Either clinician assessed or machine assessed breast density as recorded in the imaging report"}
        ),
        StructField(
            "acr_score",
            StringType(),
            True,
            {"comment": "ACR BI-RADS Score e.g. bilateral: M1; right: M2, left: M3"}
        ),
        StructField(
            "rcr_score",
            StringType(),
            True,
            {"comment": "RCR UK Score e.g. bilateral: M1; right: M2, left: M3 (mammogram); U1, U2, U3 (ultrasound)"}
        ),
        StructField(
            "index_lesion_size",
            IntegerType(),
            True,
            {"comment": "Size of the largest tumour in mm from radiology (invasive/in-situ not specified)"}
        ),
        StructField(
            "index_lesion_quadrant",
            StringType(),
            True,
            {"comment": "Quadrant of the largest tumor (index lesion) e.g. upper left, lower right"}
        ),
        StructField(
            "index_lesion_distance",
            FloatType(),
            True,
            {"comment": "Distance of the largest tumour (index lesion) from nipple"}
        ),
        StructField(
            "index_lesion_laterality",
            StringType(),
            True,
            {"comment": "Laterality of the largest tumor (index lesion) e.g. left, right"}
        ),
        StructField(
            "image_quality_notes",
            StringType(),
            True,
            {"comment": "Any notes from the radiology report on image quality including image artefacts such as motion "}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_imaging = enriched_schema(schema_pharos_imaging, 'pharos_imaging')

    def create_pharos_imaging_incr():
        begin_build(get_target_table('pharos_imaging'), ['4_prod.bronze.map_diagnosis', '4_prod.pacs.imaging_metadata'])

        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        imaging_meta = source_table("4_prod.pacs.imaging_metadata")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )


        # IMAGING: Modality aggregation within diagnostic window
        imaging = (
            imaging_meta
            .join(breast_cancer_cohort, F.col("PERSON_ID") == F.col("PersonId"), "inner")
            # Lower bound: 6 months before diagnosis; upper bound: 5 years after diagnosis
            .filter(
                (F.col("MillEventDate") > F.date_sub(F.col("brc_diag_date"), 180)) &
                (F.col("MillEventDate") <= F.date_add(F.col("brc_diag_date"), 1825))
            )
            .withColumn("days_diagnosis_imaging", F.datediff(F.col("MillEventDate"), F.col("brc_diag_date")))
            .withColumn(
                "imaging_type",
                # 0: Mammogram (Standard 2D)
                F.when((F.lower(F.col("ExaminationModality")) == "mg") & (~F.lower(F.col("ExaminationModality")).contains("tomo")), "000 - Mammogram")
                # 1: Ultrasound
                .when(F.lower(F.col("ExaminationModality")).rlike("us|ivus"), "001 - Ultrasound")
                # 2: PET
                .when(F.lower(F.col("ExaminationModality")).rlike("pt|pet|gems pet raw"), "002 - PET")
                # 3: CT
                .when(F.lower(F.col("ExaminationModality")) == "ct", "003 - CT")
                # 4: MRI
                .when(F.lower(F.col("ExaminationModality")).rlike("mr"), "004 - MRI")
                # 5: Tomosynthesis (3D Mammography)
                .when(F.lower(F.col("ExaminationModality")).contains("tomo"), "005 - Tomosynthesis")
                # 6: X-ray
                .when(F.lower(F.col("ExaminationModality")).rlike("dx|cr|xa|fl|rf|px|io"), "006 - X-ray")
                # 7: Nuclear medicine (NM)
                .when(F.lower(F.col("ExaminationModality")).rlike("nm"), "007 - Nuclear medicine (NM)")
                .otherwise("G09 - Unknown")
            )
            .select(F.col("PERSON_ID").alias("person_id"),
                    F.col("MillEventDate").alias("image_date"),
                    "days_diagnosis_imaging",
                    "imaging_type",
                    F.col("ExaminationBodyPart").alias("image_site"),
                    F.col("_src_adc_updt"))
            .dropDuplicates()
        )

        final_df = (
            imaging
            .select(
                F.lit(None).alias("imaging_id").cast(LongType()),
                F.col("person_id").cast(LongType()),
                F.col("image_date").cast(DateType()),
                F.lit(None).alias("imaging_session_id").cast(StringType()),
                F.lit(None).alias("pharosid").cast(StringType()),
                F.lit(None).alias("clinical_record_id").cast(LongType()),
                F.col("days_diagnosis_imaging").cast(IntegerType()),
                F.col("imaging_type").cast(StringType()),
                F.lit(None).alias("image_status").cast(StringType()),
                F.col("image_site").cast(StringType()),
                F.lit(None).alias("image_contrast").cast(StringType()),
                F.lit(None).alias("image_lesions").cast(IntegerType()),
                F.lit(None).alias("image_cal").cast(StringType()),
                F.lit(None).alias("breast_density").cast(StringType()),
                F.lit(None).alias("acr_score").cast(StringType()),
                F.lit(None).alias("rcr_score").cast(StringType()),
                F.lit(None).alias("index_lesion_size").cast(IntegerType()),
                F.lit(None).alias("index_lesion_quadrant").cast(StringType()),
                F.lit(None).alias("index_lesion_distance").cast(FloatType()),
                F.lit(None).alias("index_lesion_laterality").cast(StringType()),
                F.lit(None).alias("image_quality_notes").cast(StringType()),
                F.col("_src_adc_updt").alias("ADC_UPDT").cast(TimestampType()),
                F.current_timestamp().alias("date_checked").cast(TimestampType()),
                F.current_timestamp().alias("created_at").cast(TimestampType()),
                F.current_timestamp().alias("updated_at").cast(TimestampType())
            )
        )
        return complete_mappings(final_df, schema_pharos_imaging, 'pharos_imaging')

    if not SELECTED_TABLES or 'pharos_imaging' in SELECTED_TABLES:
        updated_df = create_pharos_imaging_incr()

        update_table(updated_df, get_target_table("pharos_imaging"), ["person_id", "image_date", "imaging_type"], schema_pharos_imaging, pharos_imaging_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 14, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_sample_comment = "This table serves as the biospecimen inventory for the Pharos cohort. It tracks the lineage of tissue samples from collection date through sequential laboratory procedures. It includes critical clinical context such as tissue pathology (e.g., tumour bed vs. benign), anatomical site, and timing relative to the patient's cancer diagnosis."

    schema_pharos_sample = StructType([
        StructField(
            "sample_id",
            StringType(),
            True,
            {"comment": "Unique ID for each sample"}
        ),
        StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "FK to person"}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ),
        StructField(
            "procedure_id",
            StringType(),
            True,
            {"comment": "FK to pathology"}
        ),
        StructField(
            "sample_collection_date",
            DateType(),
            True,
            {"comment": "Date of sample collection (DD-MM-YYYY)"}
        ),
        StructField(
            "days_diagnosistosample",
            IntegerType(),
            True,
            {"comment": "Time in days between diagnosis date and sample date"}
        ),
        StructField(
            "tissue_type",
            StringType(),
            True,
            {"comment": "Type of tissue collected (e.g., Non-tumour, Tumour bed, Normal, Benign)"}
        ),
        StructField(
            "tumour_sample",
            StringType(),
            True,
            {"comment": "Type of sampling specifically for tissue samples"}
        ),
        StructField(
            "sample_type",
            StringType(),
            True,
            {"comment": "Sample type stored in the Biobank"}
        ),
        StructField(
            "tissue_site",
            StringType(),
            True,
            {"comment": "Anatomical site where sample was taken from (e.g., Axillary lymph node for Sentinel nodes)"}
        ),
        StructField(
            "sample_procedure_1",
            StringType(),
            True,
            {"comment": "The first procedure performed on this specific sample"}
        ),
        StructField(
            "sample_procedure_2",
            StringType(),
            True,
            {"comment": "The second procedure performed on this specific sample"}
        ),
        StructField(
            "sample_procedure_3",
            StringType(),
            True,
            {"comment": "The third procedure performed on this specific sample"}
        ),
        StructField(
            "sample_laterality",
            StringType(),
            True,
            {"comment": "Side that the sample was taken from (Left, Right, Bilateral)"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_sample = enriched_schema(schema_pharos_sample, 'pharos_sample')

    def create_pharos_sample_incr():
        begin_build(get_target_table('pharos_sample'), ['4_prod.bronze.map_diagnosis'])

        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")    
        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )
    
        final_df = (
            breast_cancer_cohort
            .select(
                F.lit(None).cast(StringType()).alias("sample_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.lit(None).cast(StringType()).alias("procedure_id"),
                F.lit(None).cast(TimestampType()).alias("sample_collection_date"),
                F.lit(None).cast(IntegerType()).alias("days_diagnosistosample"),
                F.lit(None).cast(StringType()).alias("tissue_type"),
                F.lit(None).cast(StringType()).alias("tumour_sample"),
                F.lit(None).cast(StringType()).alias("sample_type"),
                F.lit(None).cast(StringType()).alias("tissue_site"),
                F.lit(None).cast(StringType()).alias("sample_procedure_1"),
                F.lit(None).cast(StringType()).alias("sample_procedure_2"),
                F.lit(None).cast(StringType()).alias("sample_procedure_3"),
                F.lit(None).cast(StringType()).alias("sample_laterality"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_sample, 'pharos_sample')


    if not SELECTED_TABLES or 'pharos_sample' in SELECTED_TABLES:
        updates_df = create_pharos_sample_incr()

        update_table(updates_df,get_target_table("pharos_sample"), ["person_id","sample_id"], schema_pharos_sample, pharos_sample_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 15, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_pathology_comment = "This table contains detailed pathology and surgical information related to breast cancer samples collected from participants."

    schema_pharos_pathology = StructType([
        StructField(
            "pathology_id",
            LongType(),
            nullable=True,
            metadata={"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id",
            LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            name="pharosid",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "FK to person."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Unique clincial record PharosID."}
        ),
        StructField(
            name="procedure_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "ID for the surgical or biopsy procedure that generated this pathology record."}
        ),
        StructField(
            "laterality_of_surgery",
            StringType(),
            nullable=True,
            metadata={"comment": "Laterality: Unilateral, Bilateral, Not applicable, Unknown"}
        ),
        StructField(
            "side_of_surgery",
            StringType(),
            nullable=True,
            metadata={"comment": "Side: L Left, R Right, N Not applicable, U Unknown"}
        ),
        StructField(
            "surgery_date",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Date of breast procedure"}
        ),
        StructField(
            "age_at_surgery",
            IntegerType(),
            nullable=True,
            metadata={"comment": "Age at surgery (years)"}
        ),
        StructField(
            "days_diagnosis_surgery",
            IntegerType(),
            nullable=True,
            metadata={"comment": "Days between primary diagnosis and surgery"}
        ),
        StructField(
            "breast_procedure",
            StringType(),
            nullable=True,
            metadata={"comment": "Type: Mastectomy, WLE, Excision, Biopsy, etc.)"}
        ),
        StructField(
            "breast_procedure_snomed_code",
            StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code for the breast procedure."}
        ),
        StructField(
            "breast_procedure_snomed_term",
            StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term for the breast procedure."}
        ),
        StructField(
            "somatic_mutations",
            StringType(),
            nullable=True,
            metadata={"comment": "Presence of somatic genetic mutations"}
        ),
        StructField(
            "somatic_mutations_how_detected",
            StringType(),
            nullable=True,
            metadata={"comment": "Source of sample for somatic mutation testing"}
        ),
        StructField(
            "nodal_procedure",
            StringType(),
            nullable=True,
            metadata={"comment": "Axillary procedure type"}
        ),
        StructField(
            "path_atypical",
            StringType(),
            nullable=True,
            metadata={"comment": "Atypical changes as recorded in pathology report"}
        ),
        StructField(
            "path_benign",
            StringType(),
            nullable=True,
            metadata={"comment": "Benign changes as recorded in pathology report"}
        ),
        StructField(
            "total_nodes_removed",
            IntegerType(),
            nullable=True,
            metadata={"comment": "Total number of nodes removed during surgery"}
        ),
        StructField(
            "total_positive_nodes",
            IntegerType(),
            nullable=True,
            metadata={"comment": "Total number of positive nodes (including micrometastasis; ITC is negative)"}
        ),
        StructField(
            "surgery_nodes_status",
            StringType(),
            nullable=True,
            metadata={"comment": "General Node Status"}
        ),
        StructField(
            "biopsy_bcat",
            StringType(),
            nullable=True,
            metadata={"comment": "Biopsy category (B1-B5c) or FNA category (C1-C5)"}
        ),
        StructField(
            "multifocal",
            StringType(),
            nullable=True,
            metadata={"comment": "Whether tumour was multifocal or not. Unifocal = one focus of either invasive carcinoma or pure DCIS; Multifocal = two or more foci of either invasive carcinoma or pure DCIS"}
        ),
        StructField(
            "lvi",
            StringType(),
            nullable=True,
            metadata={"comment": "Presence of lymphovascular invasion"}
        ),
        StructField(
            "margin",
            StringType(),
            nullable=True,
            metadata={"comment": "Distance to closest surgical margin in mm (excluding anterior)"}
        ),
        StructField(
            "inflammatory_infiltrate",
            StringType(),
            nullable=True,
            metadata={"comment": "Presence of inflammatory infiltrate"}
        ),
        StructField(
            "inflammatory_type",
            StringType(),
            nullable=True,
            metadata={"comment": "Type of Inflammatory Infiltrate"}
        ),
        StructField(
            "p_ajcc_edition",
            StringType(),
            nullable=True,
            metadata={"comment": "TNM staging edition used"}
        ),
        StructField(
            "pathological_t_stage",
            StringType(),
            nullable=True,
            metadata={"comment": "Pathological T stage. See ref_pathological_t_stage."}
        ),
        StructField(
            "pathological_n_stage",
            StringType(),
            nullable=True,
            metadata={"comment": "Pathological N stage. See ref_pathological_n_stage."}
        ),
        StructField(
            "pathological_m_stage",
            StringType(),
            nullable=True,
            metadata={"comment": "Pathological M stage. See ref_pathological_m_stage."}
        ),
        StructField(
            "pathological_stage",
            StringType(),
            nullable=True,
            metadata={"comment": "Pathological overall stage. See ref_pathological_stage."}
        ),
        StructField(
            "path_response",
            StringType(),
            nullable=True,
            metadata={"comment": "Path response: PR, SD, DP, CR, O Other, U Unknown"}
        ),
        StructField(
            "rcb_group",
            StringType(),
            nullable=True,
            metadata={"comment": "RCB group reported after neo-adjuvant therapy"}
        ),
        StructField(
            "rcb_volume",
            StringType(),
            nullable=True,
            metadata={"comment": "RCB volume reported after neo-adjuvant therapy"}
        ),
        StructField(
            "nodes_showing_prev_involvement",
            IntegerType(),
            nullable=True,
            metadata={"comment": "Number of nodes showing response to treatment after neoadjuvant therapy"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )

    ])

    schema_pharos_pathology = enriched_schema(schema_pharos_pathology, 'pharos_pathology')


    def create_pharos_pathology_incr():
        begin_build(get_target_table('pharos_pathology'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_procedure', '4_prod.bronze.map_person'])


        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        person = source_table("4_prod.bronze.map_person").select(F.col("person_id").alias("PERSON_ID"),"birth_year")
        procedure = source_table("4_prod.bronze.map_procedure")


        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        breast_procedure = (
            procedure
        
            .join(breast_cancer_cohort, ["PERSON_ID"], "right")
            .join(person, ["PERSON_ID"],"left")
            .withColumn("breast_procedure",
                        # The available data do not provide sufficient detail to identify specific sub-categories of mastectomy or excision, and a free-text review is required.
                        F.when(F.col("OPCS4_CODE").like("%B27%") | F.col("OPCS4_CODE").like("%B28%"), "001 - Mastectomy")
                        .when(F.col("OPCS4_CODE").rlike("B311"), "006 - Reduction mammoplasty")
                        .when(F.col("OPCS4_CODE") == "B374", "012 - Capsulectomy")
                        .when(F.col("OPCS4_CODE") == "B321", "013 - Core Needle Biopsy")
                        .when(F.col("OPCS4_CODE") == "B324", "014 - Vacuum-assisted Biopsy")
                        .when(F.col("OPCS4_CODE").isin(
                            "B323",  # Image-guided Biopsy
                            "B322",  # Biopsy of lesion of breast NEC
                            "B328",  # Other specified biopsy of breast
                            "B329"   # Unspecified biopsy of breast
                            ), "015 - Biopsy Other")
            )
            .withColumn("breast_procedure_snomed_code", F.when(F.col("breast_procedure").isNotNull(), F.col("SNOMED_CODE"))).withColumn('breast_procedure_snomed_provenance', F.when(F.col("breast_procedure").isNotNull(), F.col("SNOMED_PROVENANCE")))
            .withColumn("breast_procedure_snomed_term", F.when(F.col("breast_procedure").isNotNull(), F.col("SNOMED_TERM")))
            .withColumn("days_diagnosis_surgery", F.datediff(F.col("PROC_DT_TM"), F.col("brc_diag_date")))
            .withColumn("age_at_surgery",F.year(F.col("PROC_DT_TM")) - F.col("birth_year"))
            # .filter(col("breast_procedure").isNotNull())
            .select(F.col("PERSON_ID").alias("person_id"), F.col("PROC_DT_TM").alias("surgery_date"),"age_at_surgery","days_diagnosis_surgery","breast_procedure","breast_procedure_snomed_code", "breast_procedure_snomed_provenance","breast_procedure_snomed_term", "_src_adc_updt")
            .dropDuplicates()
        )

        final_df = (
            breast_procedure
            .select(
                F.lit(None).cast(LongType()).alias("pathology_id"),
                F.col("person_id").cast(LongType()),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.lit(None).cast(StringType()).alias("procedure_id"),
                F.lit(None).cast(StringType()).alias("laterality_of_surgery"),
                F.lit(None).cast(StringType()).alias("side_of_surgery"),
                F.col("surgery_date").cast(TimestampType()),
                F.col("age_at_surgery").cast(IntegerType()),
                F.col("days_diagnosis_surgery").cast(IntegerType()),
                F.col("breast_procedure").cast(StringType()),
                F.col("breast_procedure_snomed_code").cast(StringType()), F.col("breast_procedure_snomed_provenance"),
                F.col("breast_procedure_snomed_term").cast(StringType()),
                F.lit(None).cast(StringType()).alias("somatic_mutations"),
                F.lit(None).cast(StringType()).alias("somatic_mutations_how_detected"),
                F.lit(None).cast(StringType()).alias("nodal_procedure"),
                F.lit(None).cast(StringType()).alias("path_atypical"),
                F.lit(None).cast(StringType()).alias("path_benign"),
                F.lit(None).cast(IntegerType()).alias("total_nodes_removed"),
                F.lit(None).cast(IntegerType()).alias("total_positive_nodes"),
                F.lit(None).cast(StringType()).alias("surgery_nodes_status"),
                F.lit(None).cast(StringType()).alias("biopsy_bcat"),
                F.lit(None).cast(StringType()).alias("multifocal"),
                F.lit(None).cast(StringType()).alias("lvi"),
                F.lit(None).cast(StringType()).alias("margin"),
                F.lit(None).cast(StringType()).alias("inflammatory_infiltrate"),
                F.lit(None).cast(StringType()).alias("inflammatory_type"),
                F.lit(None).cast(StringType()).alias("p_ajcc_edition"),
                F.lit(None).cast(StringType()).alias("pathological_t_stage"),
                F.lit(None).cast(StringType()).alias("pathological_n_stage"),
                F.lit(None).cast(StringType()).alias("pathological_m_stage"),
                F.lit(None).cast(StringType()).alias("pathological_stage"),
                F.lit(None).cast(StringType()).alias("path_response"),
                F.lit(None).cast(StringType()).alias("rcb_group"),
                F.lit(None).cast(StringType()).alias("rcb_volume"),
                F.lit(None).cast(IntegerType()).alias("nodes_showing_prev_involvement"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().cast(TimestampType()).alias("date_checked"),
                F.current_timestamp().cast(TimestampType()).alias("created_at"),
                F.current_timestamp().cast(TimestampType()).alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_pathology, 'pharos_pathology')

    if not SELECTED_TABLES or 'pharos_pathology' in SELECTED_TABLES:
        updated_df = create_pharos_pathology_incr()

        update_table(updated_df, get_target_table("pharos_pathology"), ["person_id","surgery_date","breast_procedure"], schema_pharos_pathology, pharos_pathology_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 16, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_pathology_tf_comment = "This table contains detailed pathology and surgical information related to breast cancer samples collected from participants."

    schema_pharos_pathology_tf = StructType([
        StructField(
            "pathology_focus_id",
            LongType(),
            nullable=True,
            metadata={"comment": "Surrogate PK"}
        ),
        StructField(
            "pathology_id",
            LongType(),
            nullable=True,
            metadata={"comment": "FK to pathology"}
        ),
        StructField(
            name="person_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            name="pharosid",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "FK to person."}
        ),
        StructField(
            name="clinical_record_id",
            dataType=LongType(),
            nullable=True,
            metadata={"comment": "Assigned unique ID for each participant."}
        ),
        StructField(
            "invasive_present",
            StringType(),
            nullable=True,
            metadata={"comment": "Is invasive disease present?"}
        ),
        StructField(
            "morphology",
            StringType(),
            nullable=True,
            metadata={"comment": "Morphological subtype of invasive cancer"}
        ),
        StructField(
            "invasive_size_path",
            StringType(),
            nullable=True,
            metadata={"comment": "Size of invasive tumour in mm from pathology report"}
        ),
        StructField(
            "grade",
            StringType(),
            nullable=True,
            metadata={"comment": "Histological grade of invasive cancer"}
        ),
        StructField(
            "total_size_path",
            StringType(),
            nullable=True,
            metadata={"comment": "Total size of tumour in mm including in-situ disease"}
        ),
        StructField(
            "er_status",
            StringType(),
            nullable=True,
            metadata={"comment": "ER status"}
        ),
        StructField(
            "er_score_sample",
            StringType(),
            nullable=True,
            metadata={"comment": "ER score reported per sample"}
        ),
        StructField(
            "pr_status",
            StringType(),
            nullable=True,
            metadata={"comment": "PR status"}
        ),
        StructField(
            "pr_score_sample",
            StringType(),
            nullable=True,
            metadata={"comment": "PR score reported per sample"}
        ),
        StructField(
            "her2_status",
            StringType(),
            nullable=True,
            metadata={"comment": "HER2 status"}
        ),
        StructField(
            "her2_score_sample",
            StringType(),
            nullable=True,
            metadata={"comment": "HER2 score reported per sample"}
        ),
        StructField(
            "her2_fish",
            StringType(),
            nullable=True,
            metadata={"comment": "Whether or not FISH testing was undertaken"}
        ),
        StructField(
            "insitu",
            StringType(),
            nullable=True,
            metadata={"comment": "Presence of in-situ disease (includes 'Possible' for lobular neoplasia)"}
        ),
        StructField(
            "insitu_type",
            StringType(),
            nullable=True,
            metadata={"comment": "Morphological type of in-situ disease"}
        ),
        StructField(
            "dcis_subtype",
            StringType(),
            nullable=True,
            metadata={"comment": "Subtype of DCIS; multiple values separated by semi-colon (;)"}
        ),
        StructField(
            "lcis_subtype",
            StringType(),
            nullable=True,
            metadata={"comment": "Subtype of LCIS; multiple values separated by semi-colon (;)"}
        ),
        StructField(
            "insitu_grade",
            StringType(),
            nullable=True,
            metadata={"comment": "Grade of in-situ disease"}
        ),
        StructField(
            "microinvasion",
            StringType(),
            nullable=True,
            metadata={"comment": "Microinvasion < 1mm (corresponds to T1mi)"}
        ),
        StructField(
            "necrosis",
            StringType(),
            nullable=True,
            metadata={"comment": "Presence of necrosis"}
        ),
        StructField(
            name="date_checked",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Date the relevant medical record was last checked."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_pathology_tf = enriched_schema(schema_pharos_pathology_tf, 'pharos_pathology_tumourfocus')

    def create_pharos_pathology_incr():
        begin_build(get_target_table('pharos_pathology_tumourfocus'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_procedure', '4_prod.bronze.map_person'])


        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        person = source_table("4_prod.bronze.map_person").select(F.col("person_id").alias("PERSON_ID"),"birth_year")
        procedure = source_table("4_prod.bronze.map_procedure")


        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )


        final_df = (
            breast_cancer_cohort

            .select(
                F.lit(None).cast(LongType()).alias("pathology_focus_id"),
                F.lit(None).cast(LongType()).alias("pathology_id"),
                F.col("person_id").cast(LongType()),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.lit(None).cast(StringType()).alias("invasive_present"),
                F.lit(None).cast(StringType()).alias("morphology"),
                F.lit(None).cast(StringType()).alias("invasive_size_path"),
                F.lit(None).cast(StringType()).alias("grade"),
                F.lit(None).cast(StringType()).alias("total_size_path"),
                F.lit(None).cast(StringType()).alias("er_status"),
                F.lit(None).cast(StringType()).alias("er_score_sample"),
                F.lit(None).cast(StringType()).alias("pr_status"),
                F.lit(None).cast(StringType()).alias("pr_score_sample"),
                F.lit(None).cast(StringType()).alias("her2_status"),
                F.lit(None).cast(StringType()).alias("her2_score_sample"),
                F.lit(None).cast(StringType()).alias("her2_fish"),
                F.lit(None).cast(StringType()).alias("insitu"),
                F.lit(None).cast(StringType()).alias("insitu_type"),
                F.lit(None).cast(StringType()).alias("dcis_subtype"),
                F.lit(None).cast(StringType()).alias("lcis_subtype"),
                F.lit(None).cast(StringType()).alias("insitu_grade"),
                F.lit(None).cast(StringType()).alias("microinvasion"),
                F.lit(None).cast(StringType()).alias("necrosis"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().cast(TimestampType()).alias("date_checked"),
                F.current_timestamp().cast(TimestampType()).alias("created_at"),
                F.current_timestamp().cast(TimestampType()).alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_pathology_tf, 'pharos_pathology_tumourfocus')

    if not SELECTED_TABLES or 'pharos_pathology_tumourfocus' in SELECTED_TABLES:
        updated_df = create_pharos_pathology_incr()

        update_table(updated_df, get_target_table("pharos_pathology_tumourfocus"), ["person_id","pathology_focus_id"], schema_pharos_pathology_tf, pharos_pathology_tf_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 17, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_treatment_comment = "This table captures comprehensive treatment for breast cancer patients. It includes details on systemic therapies (type, drugs, intent, cycles, start/end dates, and reasons for stopping), as well as radiotherapy administration (sites, dose, fractions, and boost)."

    schema_pharos_treatment = StructType([
        StructField(
            "treatment_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "person_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "pharosid", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "FK to person"}
        ),
        StructField(
            "clinical_record_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "Unique clincial record PharosID"}
        ),
        StructField(
            "tumour_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "FK to tumour (if linkable)"}
        ),
        StructField(
            "treatment_type", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Type of treatment given"}
        ),
        StructField(
            "treatment_type_snomed_code", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "SNOMED CT code for the type of treatment given"}
        ),
        StructField(
            "treatment_type_snomed_term", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "SNOMED CT term for the type of treatment given"}
        ),
        StructField(
            "treatment_regimen", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Name of drug or treatment given"}
        ),
        StructField(
            "treatment_name", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Name of drug or treatment given"}
        ),
        StructField(
            "treatment_name_snomed_code", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "SNOMED CT code for the name of treatment drug"}
        ),
        StructField(
            "treatment_name_snomed_term", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "SNOMED CT term for the name of treatment drug"}
        ),
        StructField(
            "treatment_name_other", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Name of drug given if other selected in treatment_name"}
        ),
        StructField(
            "treatment_intent", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Treatment intent (e.g. Neoadjuvant, Adjuvant, Advanced)"}
        ),
        StructField(
            "treatment_start_date", 
            TimestampType(), 
            nullable=True, 
            metadata={"comment": "Start date of treatment"}
        ),
        StructField(
            "days_diagnosis_treatmentstart", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "Difference in days between primary diagnosis and start of treatment"}
        ),
        StructField(
            "therapy_ongoing", 
            BooleanType(), 
            nullable=True, 
            metadata={"comment": "Is the therapy ongoing?"}
        ),
        StructField(
            "months_therapy_length", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "Planned duration of therapy in days"}
        ),
        StructField(
            "treatment_end_date", 
            TimestampType(), 
            nullable=True, 
            metadata={"comment": "End date of treatment"}
        ),
        StructField(
            "days_diagnosis_treatmentend", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "Difference in days between primary diagnosis and end of treatment"}
        ),
        StructField(
            "treatment_end_reason", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Reason why the treatment was stopped (e.g. Toxicity, Progression, Protocol end, Patient choice, Death, Other, Unknown)"}
        ),
        StructField(
            "treatment_side_effects", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Any side effects from administered treatment, if multiple then  separated by semicolon (;)"}
        ),
        StructField(
            "date_treatment_last_given", 
            TimestampType(), 
            nullable=True, 
            metadata={"comment": "Date of last record of treatment administration where end date is missing"}
        ),
        StructField(
            "days_treatmentlastgiven", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "Difference in days between primary diagnosis and date of last given treatment"}
        ),
        StructField(
            "treatment_duration", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "Time in days between start and end date of treatment"}
        ),
        StructField(
            "treatment_cycles", 
            IntegerType(), 
            nullable=True, 
            metadata={"comment": "For systemic therapy given in cycles; total number of cycles given"}
        ),
        StructField(
            "clinical_trial", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Whether patient was enrolled in an interventional trial"}
        ),
        StructField(
            "radiotherapy_site", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Anatomical site(s) where radiotherapy was given"}
        ),
        StructField(
            "radiotherapy_dose", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Dose of radiotherapy administered"}
        ),
        StructField(
            "radiotherapy_fraction", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Number of radiotherapy fractions delivered"}
        ),
        StructField(
            "radiotherapy_boost", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Indicates if a boost dose of radiotherapy was given"}
        ),
        StructField(
            "radiotherapy_cd", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Procedure codes related to radiology (Barts internal use)"}
        ),
        StructField(
            "radiotherapy_desc", 
            StringType(), 
            nullable=True, 
            metadata={"comment": "Description of the procedure codes related to radiology (Barts internal use)"}
        ),
        StructField(
            "date_checked", 
            TimestampType(), 
            nullable=True, 
            metadata={"comment": "Last update timestamp."}
        ),    StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )

    ])

    schema_pharos_treatment = enriched_schema(schema_pharos_treatment, 'pharos_treatment')


    def create_pharos_treatment_incr():
        begin_build(get_target_table('pharos_treatment'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_procedure', '4_prod.bronze.map_med_admin', '4_prod.rde.rde_iqemo'])


            # Load Tables
        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        procedure = source_table("4_prod.bronze.map_procedure")
        drug = source_table("4_prod.bronze.map_med_admin")
        chemotherapy = source_table("4_prod.rde.rde_iqemo")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        # Process Chemotherapy data

        treatment_chemo = (
            chemotherapy
            .join(breast_cancer_cohort, "PERSON_ID", "inner")
            .withColumn("treatment_type", F.lit("chemotherapy"))
            .withColumn("treatment_cycles", 
                        F.when(
                            (F.col("PlannedCycles") > F.col("CycleCancelledFrom")) & (F.col("CourseFinished") == "true"), 
                            F.coalesce(F.col("PlannedCycles"), F.lit(0)) - F.coalesce(F.col("CycleCancelledFrom"), F.lit(0)))
                        .otherwise(F.col("PlannedCycles"))
                        )
            .select(
                F.col("PERSON_ID"),
                F.col("treatment_type"),
                F.col("SactName").alias("treatment_regimen"),
                F.col("Name").alias("treatment_name"),
                F.col("StartDate").alias("treatment_start_date"),
                F.col("CourseFinished").alias("therapy_ongoing"),
                F.col("EndDate").alias("treatment_end_date"),
                F.col("FinalTreatmentDate").alias("date_treatment_last_given"),
                F.col("treatment_cycles"),
                F.col("brc_diag_date"),
                F.col("_src_adc_updt")
            )
            .dropDuplicates()
        )

        # Other Systemic Therapies (Admin-based data)

        # Drug Lists for classification (Endocrine, Antibody, Inhibitors, and Supportive Care)
        endocrine_drugs = [
            72965,   # Letrozole
            10324,   # Tamoxifen
            258494,  # Exemestane
            50610,   # Goserelin
            72143,   # Raloxifene
            282357   # Fulvestrant
        ]

        antibody_drugs = [
            224905,   # Trastuzumab
            1298944,  # Pertuzumab
            253337,   # Bevacizumab
            1597876,  # Nivolumab
            993449    # Denosumab (Bone Health)
        ]
        chemotherapy_drugs = [
            194000,   # Capecitabine
            40048,    # Carboplatin
            3002,     # Cyclophosphamide
            1045453,  # Eribulin
            1160832,  # Fluorouracil (Topical)
            12574,    # Gemcitabine
            6851,     # Methotrexate
            632,      # Mitomycin C
        ]

        cdk4_6_inhibitors = [
            1601374,  # Palbociclib
            1873916,  # Ribociclib
            1740938   # Abemaciclib
        ]

        parp_inhibitors = [
            1597582,  # Olaparib
            1918231   # Niraparib
        ]

        # These are included because they showed up in treatment_name values in the cdm.
        #other_drugs = [ 
        #    141704,   # Everolimus (mTOR Inhibitor) 
        #    3264,     # Dexamethasone (Steroid) 
        #    68442,    # G-CSF (Growth Factor) 
        #    358255,   # Aprepitant (Antiemetic) 
        #    26225,    # Ondansetron (Antiemetic)  
        #    77655,    # Zoledronic acid (Bone Health) 
        #   73056,    # Risedronate (Bone Health) 
        #    11473,    # Pamidronate (Bone Health) 
        #    32915,    # Teriparatide (Bone Health) 
        #    1894,     # Calcitriol (Bone Health/Vitamin) 
        #   39786,    # Venlafaxine (Supportive Care) 
        #   25480,    # Gabapentin (Supportive Care) 
        #  6313,     # Folinic acid (Supportive Care) 
        #   8638,     # Prednisolone (Steroid) 
        #  5492,     # Hydrocortisone (Steroid) 
        #  6902      # Methylprednisolone (Steroid) 
        #]

        treatment_other = (
            drug
            .filter(
                (F.col("EVENT_TYPE_DISPLAY") == "Administered")
            )
            .join(breast_cancer_cohort, "PERSON_ID", "inner")
            .withColumn(
                "treatment_type",
                F.when(F.col("RXNORM_CUI").isin(endocrine_drugs), "001 - Endocrine")
                .when(F.col("RXNORM_CUI").isin(antibody_drugs), "003 - Antibody")
                .when(F.col("RXNORM_CUI").isin(cdk4_6_inhibitors), "004 - CDK4/6 Inhibitor")
                .when(F.col("RXNORM_CUI").isin(parp_inhibitors), "006 - PARP Inhibitor")
                .when(F.col("RXNORM_CUI").isin(chemotherapy_drugs), "002 - Chemotherapy")
                #.when(col("RXNORM_CUI").isin(other_drugs), "Other")
                .otherwise(F.lit(None))
            )
            .filter(F.col("treatment_type").isNotNull())
            .select(
                "PERSON_ID", 
                "treatment_type", 
                F.col("RXNORM_STR").alias("treatment_name"),
                F.col("SNOMED_CODE").alias("treatment_name_snomed_code"), F.col("SNOMED_PROVENANCE").alias("treatment_name_snomed_provenance"),
                F.col("SNOMED_STR").alias("treatment_name_snomed_term"),
                # WARNING: ADMIN_START/END dates represent single-day drug administrations, not the full therapy course. 
                # A freetext audit is required to validate clinical 'Treatment Start/End' dates.
                F.col("ADMIN_START_DT_TM").alias("treatment_start_date"), 
                F.col("ADMIN_END_DT_TM").alias("treatment_end_date"),
                F.col("ADMIN_END_DT_TM").alias("date_treatment_last_given"),
                F.lit(None).alias("therapy_ongoing"),
                F.lit(None).alias("treatment_cycles"), # All records in this dataset have EndDates in the past. Freetext audit is required to determine if the treatment cycles.
                F.col("brc_diag_date"),
                F.col("_src_adc_updt")
            )
        )

        # Radiotherapy
        snomed_radiotherapy_codes = [

            485444010,            # External beam radiotherapy
            485446012,            # EB - External beam radiotherapy

            2693115017,           # Radiotherapy
            5260791015,           # Radiotherapy
            2579750017,           # Radiotherapy
            1219393012,           # RT - Radiotherapy
            1784774011,           # XRT - Radiotherapy
            496531014,            # DXT - Radiotherapy

            372591000000111,      # Radiotherapy delivery (procedure)
            372601000000117,      # Radiotherapy delivery
            5034790019,           # Radiotherapy course of treatment
            261699013,            # Radiotherapy completed
            261687018,            # Post-operative course of radiotherapy
            550255014,            # Post-operative course of radiotherapy (procedure)
            261691011,            # Palliative course of radiotherapy
            550258011,            # Palliative course of radiotherapy (procedure)

            5172441014,           # Chemoradiotherapy
            3008687012,           # Combined chemotherapy and radiation therapy
            1488636014,           # Combined pre-operative chemotherapy and radiotherapy
            1468875011,           # Combined post-operative chemotherapy and radiotherapy
            262799017,            # Combined radiotherapy
            4551480018,           # Radiotherapy after chemotherapy

            1469036012,           # Stereotactic radiotherapy (procedure)
            1488795016,           # Stereotactic radiotherapy
            1894781000000115,     # SABR of prostate

            1786801012,           # Internal radiotherapy
            1786803010,           # Radiotherapy - internal
            3654594010,           # Selective internal radiotherapy (SIRT)

            236999019,            # Brachytherapy
            1786800013,           # Brachytherapy
            1490935011,           # Brachytherapy
            1490936012,           # Brachytherapy procedure
            2310961000000118,     # High dose rate brachytherapy
            4575691014,           # Low dose rate brachytherapy
            1479016010,           # Intracavitary brachytherapy
            2901374011,           # Intracavitary brachytherapy of female genital tract
            342822018,            # Gammamed brachytherapy
            342832013,            # Tantalum-182 brachytherapy
            342830017,            # Ruthenium-106 brachytherapy
            616308010,            # Iodine-125 brachytherapy
            2897350016,           # Brachytherapy using radioiodine
            2900753017,           # Brachytherapy using radioiodine
            2343691000000115,     # Ultrasound-guided prostate brachytherapy
            2343711000000118,     # Ultrasound-guided prostate brachytherapy
            2769780012,           # Fluoroscopy-guided prostate brachytherapy
            701171000000117,      # Fluoroscopy-guided prostate brachytherapy
            2981444017,           # Insertion of brachytherapy device

            2692176019,           # Radiotherapy to breast
            2694527018,           # Radiotherapy to head
            2694899018,           # Radiotherapy to neck
            2694686018,           # Radiotherapy to pelvis
            3077942015,           # Radiotherapy to thorax
            266761014,            # Radiotherapy to lacrimal gland
            261693014,            # Radiotherapy for tumour palliation
            2695397018,           # Radiotherapy by body site
            2689480017,           # Plaque radiotherapy of retina
            2695785016,           # Plaque radiotherapy of retina
            2623096012,           # Radiotherapy using radioactive plaque on eye
            262834017             # Radiotherapy seeds implanted into brain
        ]

        radiotherapy = (
            procedure
            .filter(
                F.col("SOURCE_IDENTIFIER").startswith("X65")  # Radiotherapy delivery
                | F.col("SOURCE_IDENTIFIER").startswith("Y89")  # Brachytherapy treatment
                | F.col("SOURCE_IDENTIFIER").startswith("Y91")  # External beam radiotherapy
                # additional OPCS4 codes
                | F.col("SOURCE_IDENTIFIER").isin(
                    "Y90.2",  # Radiotherapy NEC
                    "J12.3",  # Selective Internal Radiation Therapy (SIRT)
                    "C82.3",  # External beam radiotherapy to retina
                    "C82.4",  # Plaque radiotherapy to retina
                    "A61.3",  # Radiotherapy to peripheral nerve lesion
                    "C24.2",  # Radiotherapy to lacrimal gland
                    "C39.5"   # Radiotherapy to conjunctival lesion
                )

                # additional SNOMED CT radiotherapy codes
                | F.col("SOURCE_IDENTIFIER").isin(
                    *[str(x) for x in snomed_radiotherapy_codes]
                )
            )
            .select("PERSON_ID",
                    F.col("SOURCE_IDENTIFIER").alias("radiotherapy_cd"),
                    F.col("SOURCE_STRING").alias("radiotherapy_desc"),
                    F.col("PROC_DT_TM"))
        )

        treatment_radio = (
            radiotherapy
            .join(breast_cancer_cohort, "PERSON_ID", "inner")
            .withColumn("treatment_type", F.lit("006 - Radiotherapy"))
            .select(
                "PERSON_ID",
                "treatment_type",
                F.lit(None).alias("treatment_name"),
                "radiotherapy_cd",
                "radiotherapy_desc",
                F.col("PROC_DT_TM").alias("treatment_start_date"),
                F.col("PROC_DT_TM").alias("treatment_end_date"),
                F.col("PROC_DT_TM").alias("date_treatment_last_given"),
                F.lit(None).alias("therapy_ongoing"),
                F.lit(None).alias("treatment_cycles"),
                F.col("brc_diag_date"),
                F.col("_src_adc_updt")
            )
        )



        # Combine the treatment and calculate date differences
        treatment = (
            treatment_chemo
            .unionByName(treatment_other, allowMissingColumns=True)
            .unionByName(treatment_radio, allowMissingColumns=True)
            .filter(F.col("treatment_start_date") >= F.col("brc_diag_date"))
            .withColumn("days_diagnosis_treatmentstart", F.datediff(F.col("treatment_start_date"), F.col("brc_diag_date")))
            .withColumn("days_diagnosis_treatmentend", F.datediff(F.col("treatment_end_date"), F.col("brc_diag_date")))
            .withColumn("days_treatmentlastgiven", F.datediff(F.col("date_treatment_last_given"), F.col("brc_diag_date")))
            .withColumn("treatment_duration", F.datediff(F.col("treatment_end_date"), F.col("treatment_start_date")))
            .dropDuplicates()
            )


        final_df = (
            treatment
            .select(
                F.lit(None).alias("treatment_id").cast(StringType()),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).alias("pharosid").cast(StringType()),
                F.lit(None).alias("clinical_record_id").cast(LongType()),
                F.lit(None).alias("tumour_id").cast(LongType()),
                F.col("treatment_type").cast(StringType()),
                F.col("treatment_regimen").cast(StringType()),
                F.col("treatment_name").cast(StringType()),
                F.col("treatment_name_snomed_code").cast(StringType()), F.col("treatment_name_snomed_provenance"),
                F.col("treatment_name_snomed_term").cast(StringType()),
                F.lit(None).alias("treatment_name_other").cast(StringType()),
                F.lit(None).alias("treatment_intent").cast(StringType()),
                F.col("treatment_start_date").cast(TimestampType()),
                F.col("days_diagnosis_treatmentstart").cast(IntegerType()),
                F.col("therapy_ongoing").cast(StringType()),
                F.lit(None).alias("months_therapy_length").cast(IntegerType()),
                F.col("treatment_end_date").cast(TimestampType()),
                F.col("days_diagnosis_treatmentend").cast(IntegerType()),
                F.lit(None).alias("treatment_end_reason").cast(StringType()),
                F.lit(None).alias("treatment_side_effects").cast(StringType()),
                F.col("date_treatment_last_given").cast(TimestampType()),
                F.col("days_treatmentlastgiven").cast(IntegerType()),
                F.col("treatment_duration").cast(IntegerType()),
                F.col("treatment_cycles").cast(IntegerType()),
                F.lit(None).alias("clinical_trial").cast(StringType()),
                F.lit(None).alias("radiotherapy_site").cast(StringType()),
                F.lit(None).alias("radiotherapy_dose").cast(StringType()),
                F.lit(None).alias("radiotherapy_fraction").cast(StringType()),
                F.lit(None).alias("radiotherapy_boost").cast(StringType()),
                F.col("radiotherapy_cd").cast(StringType()),
                F.col("radiotherapy_desc").cast(StringType()),
                F.col("_src_adc_updt").cast(TimestampType()).alias("ADC_UPDT"),
                F.current_timestamp().cast(TimestampType()).alias("date_checked"),
                F.current_timestamp().cast(TimestampType()).alias("created_at"),
                F.current_timestamp().cast(TimestampType()).alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_treatment, 'pharos_treatment') 

    if not SELECTED_TABLES or 'pharos_treatment' in SELECTED_TABLES:
        updates_df = create_pharos_treatment_incr()

        update_table(updates_df, get_target_table("pharos_treatment"),["person_id", "treatment_type", "treatment_name", "treatment_start_date", "treatment_end_date"], schema_pharos_treatment, pharos_treatment_comment,["radiotherapy_cd","radiotherapy_desc"])
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 18, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_followup_comment = "This table tracks longitudinal outcomes for breast cancer patients, including detailed recurrence data (local and distant metastasis), survival metrics, and vital status. It captures the chronology of disease progression through event-specific dates and site locations, alongside calculated time-to-event intervals (years from diagnosis)."

    schema_pharos_followup= StructType([
        StructField(
            "followup_id",
            LongType(),
            metadata={"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id",
            LongType(),
            metadata={"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "clinical_record_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "Unique clincial record PharosID"}
        ),
        StructField(
            "local_recurrence",
            StringType(),
            metadata={"comment": "Whether or not patient had local recurrence"}
        ),
        StructField(
            "distant_metastasis",
            StringType(),
            metadata={"comment": "Whether or not patient had a distant recurrence"}
        ),
        StructField(
            "denovo_metastasis",
            StringType(),
            metadata={"comment": "Indication of distant metastasis at time of cancer diagnosis or within 6 months"}
        ),
        StructField(
            "vital_status",
            StringType(),
            metadata={"comment": "Whether patient is alive at date of last follow-up"}
        ),
        StructField(
            name="vital_status_snomed_code",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT code mapped to vital_status."}
        ),
        StructField(
            name="vital_status_snomed_term",
            dataType=StringType(),
            nullable=True,
            metadata={"comment": "SNOMED CT term mapped to the vital_status."}
        ),
        StructField(
            "date_of_death",
            TimestampType(),
            metadata={"comment": "Date of death recorded in the medical record (DD-MM-YYYY)"}
        ),
        StructField(
            "years_diagnosistodeath",
            IntegerType(),
            metadata={"comment": "Time (in years) between first diagnosis date and date of death"}
        ),
        StructField(
            "cause_of_death",
            StringType(),
            metadata={"comment": "Cause of death as recorded on death certificate or medical records"}
        ),
        StructField(
            "date_of_last_followup",
            TimestampType(),
            metadata={"comment": "Last date patient was recorded to have contact with hospital staff (DD-MM-YYYY)"}
        ),
        StructField(
            "followup_status",
            StringType(),
            metadata={"comment": "Follow up status at date last seen"}
        ),
        StructField(
            "years_lastfollowup",
            IntegerType(),
            metadata={"comment": "Time (in years) between first diagnosis and last follow-up (alive) or death (deceased)"}
        ),
        StructField(
            "lost_to_followup",
            StringType(),
            metadata={"comment": "Indication of whether patient has been lost to follow-up"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_followup = enriched_schema(schema_pharos_followup, 'pharos_followup')

    def create_pharos_followup_incr():
        begin_build(get_target_table('pharos_followup'), ['4_prod.bronze.map_death', '4_prod.bronze.map_diagnosis', '4_prod.bronze.map_encounter'])
        # Incremental update based on silver table timestamp
        death = source_table("4_prod.bronze.map_death")
        diagnosis = source_table("4_prod.bronze.map_diagnosis")

        # Identify latest contact date from hospital encounter records
        encounter = (
            source_table("4_prod.bronze.map_encounter")
            .withColumn("date_of_last_followup", F.to_timestamp(F.coalesce("DEPART_DT_TM", "ARRIVE_DT_TM")))
            .groupBy("PERSON_ID")
            .agg(F.max("date_of_last_followup").alias("date_of_last_followup"))
        )

            # Filter cohort by ICD-10 and incremental update logic
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("earliest_diagnosis_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )


        # Deduplicate death records: keep most recent per person
        death_deduped = (
            death
            .filter(F.col("DECEASED_DT_TM").isNotNull())
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("PERSON_ID").orderBy(F.col("DECEASED_DT_TM").isNotNull().desc_nulls_last(), F.col("DECEASED_DT_TM").desc_nulls_last(), F.col("ADC_UPDT").desc_nulls_last())
            ))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Process death records and calculate survival time (years)
        death_processed = (
            death_deduped
            .join(breast_cancer_cohort, "PERSON_ID", "right")
            .withColumn(
                "vital_status",
                F.when(F.col("DECEASED_DT_TM").isNotNull(), F.lit("001 - Deceased"))
                .otherwise(F.lit("000 - Alive"))
            )
            .withColumn(
                "years_diagnosistodeath",
                F.when(F.col("DECEASED_DT_TM").isNotNull(),
                    F.datediff(F.col("DECEASED_DT_TM"), F.col("earliest_diagnosis_date")).cast(DoubleType()) / 365)
                .otherwise(F.lit(None))
            )
            .select(
                "PERSON_ID",
                "vital_status",
                F.col("DECEASED_DT_TM").alias("date_of_death"),
                "years_diagnosistodeath",
                "earliest_diagnosis_date",
                "_src_adc_updt"
            )
        )


        # Merge mortality data with encounter history
        final_df = (
            death_processed
            .join(encounter, "PERSON_ID", "left")
            # Follow-up duration calculation (Diagnosis to last contact)
            .withColumn(
                "years_lastfollowup",
                F.datediff(F.col("date_of_last_followup"), F.col("earliest_diagnosis_date")).cast(DoubleType()) / 365
            )
            .select(
                # Recurrence/Metastasis placeholders reserved for future free-text audit results
                F.lit(None).cast(StringType()).alias("followup_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.lit(None).cast(StringType()).alias("local_recurrence"),
                F.lit(None).cast(StringType()).alias("distant_metastasis"),
                F.lit(None).cast(StringType()).alias("denovo_metastasis"),
                F.col("vital_status").cast(StringType()),
                F.col("date_of_death").cast(TimestampType()),
                F.col("years_diagnosistodeath").cast(IntegerType()),
                F.lit(None).cast(StringType()).alias("cause_of_death"),
                F.col("date_of_last_followup").cast(TimestampType()),
                F.lit(None).cast(StringType()).alias("followup_status"),
                F.col("years_lastfollowup").cast(IntegerType()),
                F.lit(None).cast(StringType()).alias("lost_to_followup"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.lit(F.current_timestamp()).cast(TimestampType()).alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_followup, 'pharos_followup')

    if not SELECTED_TABLES or 'pharos_followup' in SELECTED_TABLES:
        updates_df = create_pharos_followup_incr()
        update_table(updates_df, get_target_table("pharos_followup"), "person_id", schema_pharos_followup, pharos_followup_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 19, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_recurrence_comment = "Stores cancer recurrence information for each person, including recurrence details and record audit timestamps."

    schema_pharos_recurrence = StructType([
        StructField(
            "recurrence_id",
            IntegerType(),
            True,
            {"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "FK to person"}
        ),
        StructField(
            "recurrence_date",
            TimestampType(),
            metadata={"comment": "Date of local recurrence"}
        ),
        StructField(
            "days_diagnosis_recurrence",
            IntegerType(),
            metadata={"comment": "Date of local recurrence"}
        ),
        StructField(
            "recurrence_sites",
            StringType(),
            metadata={"comment": "Site: Ipsilateral breast, Ipsilateral axilla, Chest wall, etc."}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_recurrence = enriched_schema(schema_pharos_recurrence, 'pharos_recurrence')

    def create_pharos_recurrence_incr():
        begin_build(get_target_table('pharos_recurrence'), ['4_prod.bronze.map_diagnosis'])

        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")    
        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )
    
        final_df = (
            breast_cancer_cohort
            .select(
                F.lit(None).cast(IntegerType()).alias("recurrence_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(TimestampType()).alias("recurrence_date"),
                F.lit(None).cast(IntegerType()).alias("days_diagnosis_recurrence"),
                F.lit(None).cast(StringType()).alias("recurrence_sites"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_recurrence, 'pharos_recurrence')


    if not SELECTED_TABLES or 'pharos_recurrence' in SELECTED_TABLES:
        updates_df = create_pharos_recurrence_incr()

        update_table(updates_df, get_target_table("pharos_recurrence"), "person_id", schema_pharos_recurrence,pharos_recurrence_comment)

except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 20, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_metastasis_comment = (
        "Stores cancer metastasis information for each person, including metastasis details and record audit timestamps."
    )

    schema_pharos_metastasis = StructType([
        StructField(
            "metastasis_id",
            IntegerType(),
            True,
            {"comment": "Surrogate PK"}
        ),
            StructField(
            "person_id",
            LongType(),
            True,
            {"comment": "Assigned unique ID for each participant"}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "FK to followup/person"}
        ),
        StructField(
            "metastasis_date",
            TimestampType(),
            True,
            {"comment": "Date of distant recurrence episode"}
        ),
        StructField(
            "days_diagnosis_metastasis",
            IntegerType(),
            True,
            {"comment": "Days between primary diagnosis and this metastasis"}
        ),
        StructField(
            "metastasis_site",
            StringType(),
            True,
            {"comment": "Anatomical site of metastasis"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            "date_checked",
            TimestampType(),
            True,
            {"comment": "Date the relevant medical record was last checked"}
        ),
        StructField(
            "created_at",
            TimestampType(),
            True,
            {"comment": "Row creation timestamp"}
        ),
        StructField(
            "updated_at",
            TimestampType(),
            True,
            {"comment": "Row last-updated timestamp"}
        )
    ])

    schema_pharos_metastasis = enriched_schema(schema_pharos_metastasis, 'pharos_metastasis')


    def create_pharos_metastasis_incr():
        begin_build(get_target_table('pharos_metastasis'), ['4_prod.bronze.map_diagnosis'])

        map_diagnosis = source_table("4_prod.bronze.map_diagnosis")

        breast_cancer_cohort = (
            map_diagnosis
            .filter(
                (
                    F.col("ICD10_CODE").like("C50%") |
                    F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331)
                )
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        final_df = (
            breast_cancer_cohort
            .select(
                F.lit(None).cast(IntegerType()).alias("metastasis_id"),
                F.col("PERSON_ID").cast(LongType()).alias("person_id"),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(TimestampType()).alias("metastasis_date"),
                F.lit(None).cast(IntegerType()).alias("days_diagnosis_metastasis"),
                F.lit(None).cast(StringType()).alias("metastasis_site"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_metastasis, 'pharos_metastasis')


    if not SELECTED_TABLES or 'pharos_metastasis' in SELECTED_TABLES:
        updates_df = create_pharos_metastasis_incr()

        update_table(
            updates_df,get_target_table("pharos_metastasis"),"person_id",schema_pharos_metastasis,pharos_metastasis_comment
        )
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 21, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_comorbidities_comment = "This table records the co-existing medical conditions (comorbidities) of participants in the Pharos cohort, with a specialized focus on diabetes management. It captures diagnostic data using ICD-10 standards, tracks the temporal relationship (pre- vs. post-cancer diagnosis) of each condition, and documents longitudinal diabetes care including associated medications. The data is structured to support analysis of how underlying health status impacts cancer treatment outcomes and patient survival."

    schema_pharos_comorbidities = StructType([
        StructField(
            "comorbidity_id",
            StringType(),
            True,
            {"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id", 
            LongType(), 
            True, 
            {"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "pharosid",
            StringType(),
            True,
            {"comment": "FK to person"}
        ),
        StructField(
            "clinical_record_id", 
            LongType(), 
            nullable=True, 
            metadata={"comment": "Unique clincial record PharosID"}
        ),
        StructField(
            "comorbidities", 
            StringType(), 
            True, 
            {"comment": "Whether comorbidities are present"}
        ),
        StructField(
            "icd_code", 
            StringType(), 
            True, 
            {"comment": "ICD code for each condition (from ICD-10)"}
        ),
        StructField(
            "snomed_code", 
            StringType(), 
            True, 
            {"comment": "SNOMED CT code for each condition"}
        ),
        StructField(
            "snomed_term", 
            StringType(), 
            True, 
            {"comment": "SNOMED CT term for each condition"}
        ),
        StructField(
            "comorbidity_name", 
            StringType(), 
            True, 
            {"comment": "ICD description name for condition (from ICD-10)"}
        ),
        StructField(
            "comorbidity_temporality", 
            StringType(), 
            True, 
            {"comment": "Whether comorbidity was diagnosed before or after cancer diagnosis"}
        ),
        StructField(
            "diabetes", 
            StringType(), 
            True, 
            {"comment": "Whether patient is diabetic according to medical record"}
        ),
        StructField(
            "diabetes_temporality", 
            StringType(), 
            True, 
            {"comment": "Whether diabetes was diagnosed before or after cancer diagnosis"}
        ),
        StructField(
            "date_assessed", 
            TimestampType(), 
            True, 
            {"comment": "Date that this condition was entered"}
        ),
        StructField(
            "days_diagnosis_comor", 
            IntegerType(), 
            True, 
            {"comment": "Time in days between date diagnosis and date of comorbidity condition assessment"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked", 
            TimestampType(), 
            nullable=True, 
            metadata={"comment": "Date the relevant medical record was last checked."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )

    ])

    schema_pharos_comorbidities = enriched_schema(schema_pharos_comorbidities, 'pharos_comorbidities')

    def create_pharos_comorbidity_incr():
        begin_build(get_target_table('pharos_comorbidities'), ['4_prod.bronze.map_diagnosis', get_target_table('pharos_treatment')])


        diagnosis = source_table("4_prod.bronze.map_diagnosis")
        treatment = (
            source_table(get_target_table("pharos_treatment"))
            .select(F.col("person_id").alias("PERSON_ID"),"treatment_start_date","treatment_end_date")
            .groupBy("person_id")
            .agg(
                F.min("treatment_start_date").alias("treatment_start_date"),
                F.max("treatment_end_date").alias("treatment_end_date")
            )
        )

        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        # The following medical condition with ICD10 codes are excluded,  
        code_excluded = [
            "C5*",  # Breast cancer
            "C77", "C78", "C79",  # Metastasis
            "S", "T",  # Injury, poisoning and certain other consequences of external causes
            "V", "W", "X", "Y",  # External causes of morbidity and mortality
            "Z", # Factors influencing health status and contact with health services
            "U" # XXII Codes for special purposes
        ]

        # Convert the wildcard list into a single regex for rlike  
        excluded_regex = r"^(?:{})".format("|".join(code_excluded))  
        window = Window.partitionBy("PERSON_ID","ICD10_CODE").orderBy(F.col("ICD10_CODE").isNotNull().desc_nulls_last(), F.col("earliest_diagnosis_date").asc_nulls_last(), (F.col("SOURCE_STRING") == F.lower(F.col("SOURCE_STRING"))).desc_nulls_last(), F.col("SOURCE_STRING").asc_nulls_last(), F.col("SNOMED_CODE").asc_nulls_last(), F.to_json(F.col("SNOMED_PROVENANCE")).asc_nulls_last())
        comorbidity = (
            diagnosis
            .filter((~F.col("ICD10_CODE").rlike(excluded_regex)))
            .join(breast_cancer_cohort, "PERSON_ID", "inner")
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .join(treatment, ["PERSON_ID"], "left") 
        
            # COMORBIDITIES --------------- 
            .withColumn(
                "comorbidities",
                F.when(F.col("ICD10_CODE").isNotNull(), F.lit("G01 Yes"))
                .otherwise(F.lit("G09 Unknown"))
            )
            .withColumn(
                "comorbidity_temporality",
                F.when(F.col("ICD10_CODE").isNull(), F.lit("G09 Unknown temporality"))
                .when(F.col("earliest_diagnosis_date") < F.col("brc_diag_date"), F.lit("000 Pre-diagnosis"))
                .when(
                    (F.col("earliest_diagnosis_date") >= F.col("brc_diag_date")) & 
                    (F.col("earliest_diagnosis_date") <= F.col("treatment_start_date")), F.lit("001 Post-diagnosis"))
                .when(
                    (F.col("earliest_diagnosis_date") >= F.col("treatment_start_date")) & 
                    (F.col("earliest_diagnosis_date") <= F.col("treatment_end_date")), F.lit("002 During treatment"))
                .when(F.col("earliest_diagnosis_date") > F.col("treatment_end_date"), F.lit("001 Post-diagnosis"))
            )

            # DIABETES ----------------
            .withColumn(
                "diabetes",
                F.when(F.col("ICD10_CODE").like("E10%"), "001 Type I diabetes")
                .when(
                    (F.col("ICD10_CODE").like("E11%")) |
                    (F.col("OMOP_CONCEPT_ID") == 45757508),
                    "002 Type II diabetes"
                )
                .when(
                    (F.col("ICD10_CODE").like("R73%")) |
                    (F.col("OMOP_CONCEPT_ID").isin(44808385, 37018196)),
                    "003 Pre-diabetic/borderline"
                )
                # Probably this other type of diabetes can be included in the comorbidities (check)
                # .when(
                #     (col("ICD10_CODE").like("E14%")) | # Unspecified diabetes mellitus
                #     (col("ICD10_CODE").like("E13%")) | # Other specified diabetes mellitus
                #     (col("ICD10_CODE").like("E12%")) | # Malnutrition-related diabetes mellitus,
                #     "4 Other diabetes types"
                # )
                .otherwise(F.lit("G09 Unknown"))
            )
            .withColumn(
                "diabetes_temporality",
                F.when(
                    (F.col("diabetes")!= "G09 Unknown") & (F.col("earliest_diagnosis_date") < F.col("brc_diag_date")), "000 Pre-diagnosis")
                .when(
                    (F.col("diabetes")!= "G09 Unknown") &
                    (F.col("earliest_diagnosis_date") >= F.col("brc_diag_date")) & 
                    (F.col("earliest_diagnosis_date") <= F.col("treatment_start_date")), F.lit("001 Post-diagnosis"))
                .when(
                    (F.col("diabetes")!= "G09 Unknown") &
                    (F.col("earliest_diagnosis_date") >= F.col("treatment_start_date")) & 
                    (F.col("earliest_diagnosis_date") <= F.col("treatment_end_date")), F.lit("002 During treatment"))
                .when(
                    (F.col("diabetes")!= "G09 Unknown") &
                    (F.col("earliest_diagnosis_date") > F.col("treatment_end_date")), F.lit("001 Post-diagnosis"))
                .otherwise(F.lit("G09 Unknown"))
            )
            .withColumn("days_diagnosis_comor", F.datediff(F.col("earliest_diagnosis_date"), F.col("brc_diag_date")))
        )


        final_df = (
            comorbidity
            .select(
                F.lit(None).cast(StringType()).alias("comorbidity_id"),
                F.col("PERSON_ID").alias("person_id").cast(LongType()),
                F.lit(None).cast(StringType()).alias("pharosid"),
                F.lit(None).cast(LongType()).alias("clinical_record_id"),
                F.col("comorbidities").cast(StringType()),
                F.col("ICD10_CODE").alias("icd_code").cast(StringType()),
                F.col("ICD10_TERM").alias("comorbidity_name").cast(StringType()),
                F.col("SNOMED_CODE").alias("snomed_code").cast(StringType()), F.col("SNOMED_PROVENANCE").alias("snomed_provenance"),
                F.col("SNOMED_TERM").alias("snomed_term").cast(StringType()),
                F.col("comorbidity_temporality").cast(StringType()),
                F.col("diabetes").cast(StringType()),
                F.col("diabetes_temporality").cast(StringType()),
                F.col("earliest_diagnosis_date").alias("date_assessed").cast(TimestampType()),
                F.col("days_diagnosis_comor").cast(IntegerType()),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked").cast(TimestampType()),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
            .dropDuplicates()  
        )
        return complete_mappings(final_df, schema_pharos_comorbidities, 'pharos_comorbidities')

    if not SELECTED_TABLES or 'pharos_comorbidities' in SELECTED_TABLES:
        updated_df = create_pharos_comorbidity_incr()
        update_table(updated_df, get_target_table("pharos_comorbidities"),["person_id", "icd_code"], schema_pharos_comorbidities, pharos_comorbidities_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 22, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_meds_comment = "This table records the co-existing medical conditions (comorbidities) of participants in the Pharos cohort, with a specialized focus on diabetes management. It captures diagnostic data using ICD-10 standards, tracks the temporal relationship (pre- vs. post-cancer diagnosis) of each condition, and documents longitudinal diabetes care including associated medications. The data is structured to support analysis of how underlying health status impacts cancer treatment outcomes and patient survival."

    schema_pharos_meds = StructType([
        StructField(
            "medication_id", 
            LongType(), 
            True, 
            {"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id", 
            LongType(), 
            True, 
            {"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "clinical_record_id", 
            LongType(), 
            True, 
            {"comment": "Unique clincial record PharosID"}
        ),
        StructField(
            "medication_name", 
            StringType(), 
            True, 
            {"comment": "Name of the non-cancer medication the patient was taking at the time of cancer diagnosis, one medication per row, only one patient"}
        ),
        StructField(
            "medication_name_snomed_code", 
            StringType(), 
            True, 
            {"comment": "SNOMED CT code for medication name"}
        ),
        StructField(
            "medication_name_snomed_term", 
            StringType(), 
            True, 
            {"comment": "SNOMED CT term for medication name"}
        ),
        StructField(
            "medication_category", 
            StringType(), 
            True, 
            {"comment": "Category of the non-cancer medication the patient was taking at the time of cancer diagnosis"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Max source ADC_UPDT for incremental watermarking"}
        ),
        StructField(
            "date_checked", 
            TimestampType(), 
            True, 
            {"comment": "Date medical record regarding Non-cancer Medications data was checked DD-MM-YYYY"}
        )
    ])

    schema_pharos_meds = enriched_schema(schema_pharos_meds, 'pharos_medication')


    def create_pharos_med_incr():
        begin_build(get_target_table('pharos_medication'), ['4_prod.bronze.map_diagnosis', '4_prod.bronze.map_med_admin'])

        diagnosis = source_table("4_prod.bronze.map_diagnosis")

        medicine = source_table("4_prod.bronze.map_med_admin")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("earliest_diagnosis_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        T1DM = [
            "51428",    # Insulin Aspart
            "86009",    # Insulin Lispro
            "400008",   # Insulin Glulisine
            "253181",   # Insulin Isophane
            "253182",   # Human Regular Insulin
            "139825",   # Insulin Detemir
            "274783",   # Insulin Glargine
            "1670007",  # Insulin Degludec
            "261420",   # Insulin Lispro Protamine Mix
            "2004040",  # Insulin Aspart Protamine Mix
            "1650023"   # Insulin Isophane–Regular Mix
        ]

        T2DM = [
            "4815",      # Glyburide
            "4821",      # Glipizide
            "6809",      # Metformin
            "10635",     # Tolbutamide
            "16681",     # Acarbose
            "25789",     # Glimepiride
            "33738",     # Pioglitazone
            "60548",     # Exenatide
            "73044",     # Repaglinide
            "475968",    # Liraglutide
            "593411",    # Sitagliptin
            "607999",    # Metformin and pioglitazone
            "729717",    # Metformin and sitagliptin
            "857974",    # Saxagliptin
            "1100699",   # Linagliptin
            "1243019",   # Linagliptin / metformin
            "1368001",   # Alogliptin
            "1368384",   # Alogliptin-metformin
            "1373458",   # Canagliflozin
            "1440051",   # Lixisenatide
            "1488564",   # Dapagliflozin
            "1545653",   # Empagliflozin
            "1551291",   # Dulaglutide
            "1991302",   # Semaglutide
            "2601723"    # Tirzepatide
        ]

        OTHER_MEDS = [
            "67108",    # Enoxaparin
            "32968",    # Clopidogrel
            "4603",     # Furosemide
            "235473",   # Heparin
            "298869",   # Eplerenone
            "104462",   # Heparin Flush
            "42463",    # Pravastatin
            "29046",    # Lisinopril
            "52769",    # Milrinone
            "6628",     # Mannitol
            "337623",   # Dornase Alfa
            "1592737",  # Nintedanib
            "8814",     # Epoprostenol
            "136411",   # Sildenafil
            "358263",   # Tadalafil
            "214618"    # Hydrochlorothiazide-Lisinopril
        ]

        all_diabetes_meds = T1DM + T2DM + OTHER_MEDS

        med = (
            medicine
            .join(breast_cancer_cohort, "PERSON_ID", "inner")
            .filter(F.col("RXNORM_CUI").isin(all_diabetes_meds))
            # Time window for the medication record selection (<=90 days)
            .filter(
                (F.col("ADMIN_START_DT_TM") <= F.col("earliest_diagnosis_date")) &
                (F.datediff(F.col("earliest_diagnosis_date"), F.col("ADMIN_START_DT_TM")) <= 90)
            )
            .withColumn(
                "medication_category",
                F.when(F.col("RXNORM_CUI").isin("32968", "52769"), "001 - Heart related medications")  # Clopidogrel, Milrinone
                .when(F.col("RXNORM_CUI").isin("42463"), "002 - Statins")  # Pravastatin
                .when(
                    F.col("RXNORM_CUI").isin(
                        "29046",   # Lisinopril
                        "214618",  # Hydrochlorothiazide-Lisinopril
                        "298869",  # Eplerenone
                        "8814",    # Epoprostenol
                        "136411",  # Sildenafil
                        "358263"   # Tadalafil
                    ),
                    "003 - Hypertension drugs"
                )
                .when(F.col("RXNORM_CUI").isin("67108", "235473", "104462"), "004 - Anticoagulants")  # Enoxaparin, Heparin, Heparin Flush
                .when(F.col("RXNORM_CUI").isin("6628"), "005 - Inhalers and respiratory meds")  # Mannitol
                .when(F.col("RXNORM_CUI").isin("337623", "1592737"), "006 - COPD medications")  # Dornase Alfa, Nintedanib
                .when(F.col("RXNORM_CUI").isin(T1DM), "007 - T1DM Meds")
                .when(F.col("RXNORM_CUI").isin(T2DM), "008 - T2DM Meds")
            )
            .select(
                F.col("PERSON_ID").alias("person_id"),
                F.col("RXNORM_STR").alias("medication_name"),
                F.col("SNOMED_CODE").alias("medication_name_snomed_code"), F.col("SNOMED_PROVENANCE").alias("medication_name_snomed_provenance"),
                F.col("SNOMED_STR").alias("medication_name_snomed_term"),
                F.col("medication_category"),
                F.col("_src_adc_updt")
            )  
            .dropDuplicates()
        )

        final_df =(
            med
            .select(
                F.lit(None).alias("medication_id").cast(LongType()),
                F.col("person_id").cast(LongType()),
                F.lit(None).alias("pharosid").cast(StringType()),
                F.lit(None).alias("clinical_record_id").cast(LongType()),
                F.col("medication_name").cast(StringType()),
                F.col("medication_name_snomed_code").cast(StringType()), F.col("medication_name_snomed_provenance"),
                F.col("medication_name_snomed_term").cast(StringType()),
                F.col("medication_category").cast(StringType()),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked").cast(TimestampType()),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )  
        )

        return complete_mappings(final_df, schema_pharos_meds, 'pharos_medication')

    if not SELECTED_TABLES or 'pharos_medication' in SELECTED_TABLES:
        updated_df = create_pharos_med_incr()
        update_table(updated_df, get_target_table("pharos_medication"), ["person_id", "medication_name"], schema_pharos_meds, pharos_meds_comment)
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 24, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise


# COMMAND ----------

try:
    pharos_ph_comment = "This table contains a harmonised and standardised record of personal history of cancer diagnoses."

    schema_pharos_ph = StructType([
        StructField(
            "history_id", 
            LongType(), 
            True, 
            {"comment": "Surrogate PK"}
        ),
        StructField(
            "person_id", 
            LongType(), 
            True, 
            {"comment": "Assigned unique ID for each participant (TBC)"}
        ),
        StructField(
            "pharosid", 
            StringType(), 
            True, 
            {"comment": "FK to person"}
        ),
        StructField(
            "clinical_record_id", 
            LongType(), 
            True, 
            {"comment": "Unique clincial record PharosID"}
        ),
        StructField(
            "cancer_type_code", 
            StringType(), 
            True, 
            {"comment": "Anatomical or diagnostic category of the historical cancer"}
        ),
        StructField(
            "Source_cd", 
            StringType(), 
            True, 
            {"comment": "Original code for the person history (Barts internal use)."}
        ),
        StructField(
            "cancer_type_detail", 
            StringType(), 
            True, 
            {"comment": "Free-text detail if not in reference list"}
        ),
        StructField(
            "cancer_type_snomed_code", 
            StringType(), 
            True, 
            {"comment": "SNOMED_CT code for the personal cancer history."}
        ),
        StructField(
            "cancer_type_snomed_term", 
            StringType(), 
            True, 
            {"comment": "SNOMED_CT term for the personal cancer history."}
        ),
        StructField(
            "date_checked", 
            StringType(), 
            True, 
            {"comment": "Date the medical record was reviewed for cancer history data DD-MM-YYYY"}
        ),
        StructField(
            "ADC_UPDT",
            TimestampType(),
            nullable=True,
            metadata={"comment": "Last update timestamp."}
        ),
        StructField(
            name="created_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row creation timestamp."}
        ),
        StructField(
            name="updated_at",
            dataType=TimestampType(),
            nullable=True,
            metadata={"comment": "Row last-updated timestamp."}
        )
    ])

    schema_pharos_ph = enriched_schema(schema_pharos_ph, 'pharos_personal_history')

    def create_pharos_ph_incr():
        begin_build(get_target_table('pharos_personal_history'), ['4_prod.bronze.map_diagnosis'])

        diagnosis = source_table("4_prod.bronze.map_diagnosis")

        # Get the breast cancer cohort
        breast_cancer_cohort = (
            diagnosis
            .filter(
                (F.col("ICD10_CODE").like("C50%") | F.col("OMOP_CONCEPT_ID").isin(45768522, 35624616, 602331))
            )
            .filter(F.col("PERSON_ID").isNotNull())
            .groupBy("PERSON_ID")
            .agg(
                F.min("earliest_diagnosis_date").alias("brc_diag_date"),
                F.max("ADC_UPDT").alias("_src_adc_updt")
            )
        )

        omop_map = {
            4333465:  "006 - Brain cancer",                  # History of malignant neoplasm of nervous system
            46269969: "006 - Brain cancer",                  # History of cancer metastatic to brain
            3175032:  "018 - Head and neck cancer",          # History of cancer of ear
            3190039:  "018 - Head and neck cancer",          # History of cancer of head and neck
            4333345:  "018 - Head and neck cancer",          # History of malignant neoplasm of head and/or neck
            3189194:  "034 - Oral cancer",                   # History of cancer of mouth
            3184244:  "020 - Laryngeal cancer",              # History of cancer of larynx
            46270611: "044 - Salivary gland cancer",         # History of malignant neoplasm of parotid gland
            1246982:  "G09 - Unknown",                       # History of metastatic cancer
            3181707:  "045 - Sinonasal cancer",              # History of cancer of nares
            3184017:  "045 - Sinonasal cancer",              # History of sinus cancer
            4212564:  "023 - Lung cancer",                   # History of malignant neoplasm of bronchus
            4324190:  "007 - Breast cancer",                 # History of malignant neoplasm of breast
            4187205:  "023 - Lung cancer",                   # History of malignant neoplasm of lung
            4187206:  "023 - Lung cancer",                   # History of malignant neoplasm of trachea
            4323367:  "040 - Pleural cancer",                # History of malignant mesothelioma
            4178640:  "040 - Pleural cancer",                # History of malignant neoplasm of pleura
            4179069:  "G09 - Unknown",                       # History of malignant neoplasm of thoracic cavity structure
            4177071:  "025 - Mediastinal cancer",            # History of malignant neoplasm of mediastinum
            46273376: "047 - Small bowel cancer",            # History of cancer of ampulla of duodenum
            35610791: "G09 - Unknown",                       # History of malignant neoplasm of digestive organ
            4190633:  "011 - Colorectal cancer",             # History of malignant neoplasm of gastrointestinal tract
            3184515:  "G09 - Unknown",                       # History of splenic cancer
            46273480: "011 - Colorectal cancer",             # History of rectosigmoid junction cancer
            4327107:  "010 - Colon cancer",                  # History of malignant neoplasm of colon
            4324189:  "043 - Rectal cancer",                 # History of malignant neoplasm of rectum
            37016142: "002 - Anal cancer",                   # History of malignant neoplasm of anus
            3189077:  "022 - Liver cancer",                  # History of liver cancer
            4180749:  "042 - Prostate cancer",               # History of malignant neoplasm of prostate
            3169330:  "042 - Prostate cancer",               # Personal history of prostate cancer
            43021271: "050 - Testicular cancer",             # History of malignant neoplasm of testis
            4327872:  "039 - Penile cancer",                 # History of malignant neoplasm of penis
            4187203:  "G09 - Unknown",                       # History of malignant neoplasm of female genital organ
            4323346:  "G09 - Unknown",                       # History of malignant neoplasm of uterine adnexa
            46273417: "041 - Primary peritoneal cancer",     # History of malignant neoplasm of peritoneum
            4178782:  "008 - Cervical cancer",               # History of malignant neoplasm of cervix
            45763611: "012 - Endometrial cancer",            # History of carcinosarcoma of uterus
            37159719:  "012 - Endometrial cancer",           # History of choriocarcinoma
            4179084:  "012 - Endometrial cancer",            # History of malignant neoplasm of uterine body
            4325186:  "055 - Vaginal cancer",                # History of malignant neoplasm of vagina
            4181024:  "056 - Vulvar cancer",                 # History of malignant neoplasm of vulva
            4216132:  "004 - Bladder cancer",                # History of malignant neoplasm of urinary system
            4325868:  "053 - Upper urinary tract cancer",    # History of malignant neoplasm of ureter
            46273500: "054 - Urethral cancer",               # History of cancer of urethra
            3174258:  "052 - Thyroid cancer",                # History of thyroid cancer
            4197758:  "G09 - Unknown",                       # History of malignant neoplasm of endocrine gland
            46270077: "031 - Neuroendocrine tumour",         # History of neuroendocrine malignant neoplasm
            4058705:  "026 - Melanoma",                      # H/O Malignant melanoma
            4179242:  "032 - Non-melanoma skin cancer",      # History of malignant neoplasm of skin
            3176981:  "005 - Bone cancer",                   # History of bone cancer
            4180113:  "005 - Bone cancer",                   # History of malignant neoplasm of bone
            4325206:  "005 - Bone cancer",                   # History of osteosarcoma
            4323212:  "048 - Soft tissue sarcoma",           # History of Kaposi's sarcoma
            46273501: "048 - Soft tissue sarcoma",           # History of malignant neoplasm of retroperitoneum
            46273380: "048 - Soft tissue sarcoma",           # History of rhabdomyosarcoma
            3176052:  "048 - Soft tissue sarcoma",           # History of sarcoma
            46270545: "048 - Soft tissue sarcoma",           # History of soft tissue sarcoma
            44782983: "G09 - Unknown",                       # History of malignant hematologic neoplasm
            3180053:  "024 - Lymphoma",                      # History of Hodgkins disease
            4180131:  "017 - Germ cell tumour",              # History of malignant neoplasm of epididymis
            4190635:  "G09 - Unknown",                       # History of malignant neoplasm of male genital organ
            4332932:  "009 - Childhood cancer",              # History of neuroblastoma
            44782997: "009 - Childhood cancer"               # History of primitive neuroectodermal tumor
        }

        icd10_map = {
            "Z85.0": "G09 - Unknown",                       # digestive organs
            "Z85.1": "023 - Lung cancer",                   # trachea, bronchus, lung
            "Z85.2": "G09 - Unknown",                       # other respiratory/intrathoracic
            "Z85.3": "007 - Breast cancer",                 # breast
            "Z85.4": "G09 - Unknown",                       # genital organs
            "Z85.5": "G09 - Unknown",                       # urinary tract
            "Z85.6": "021 - Leukaemia",                     # leukaemia
            "Z85.7": "G09 - Unknown",                       # lymphoid/haematopoietic
            "Z85.8": "G09 - Unknown",                       # other organs
            "Z85.9": "G09 - Unknown"                        # unspecified
        }



        combined_cancer_map = {
            **{str(k): v for k, v in omop_map.items()},
            **icd10_map
        }

        mapping_df = spark.createDataFrame(
            [(str(k), v) for k, v in combined_cancer_map.items()],
            "code string, cancer_type_code string"
        )

        # Normalize diagnosis codes
        diagnosis = diagnosis.withColumn(
            "code",
            F.col("OMOP_CONCEPT_ID").cast("string")
        )

        person_history = (
            diagnosis
            .join(breast_cancer_cohort, "PERSON_ID", "left")
            .join(mapping_df, "code", "inner")
        )

        final_df = (
            person_history
            .filter(F.col("cancer_type_code").isNotNull())
            .select(
                F.lit(None).cast(LongType()).alias("history_id"),
                F.col("person_id"),
                F.lit(None).alias("pharosid"),
                F.lit(None).alias("clinical_record_id"),
                F.col("cancer_type_code"),
                F.col("code").alias("Source_cd"),
                F.col("SOURCE_STRING").alias("cancer_type_detail"),
                F.col("SNOMED_CODE").alias("cancer_type_snomed_code"), F.col("SNOMED_PROVENANCE").alias("cancer_type_snomed_provenance"),
                F.col("SNOMED_TERM").alias("cancer_type_snomed_term"),
                F.col("_src_adc_updt").alias("ADC_UPDT"),
                F.current_timestamp().alias("date_checked"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
        )

        return complete_mappings(final_df, schema_pharos_ph, 'pharos_personal_history')

    if not SELECTED_TABLES or 'pharos_personal_history' in SELECTED_TABLES:
        updated_df = create_pharos_ph_incr()

        update_table(updated_df, get_target_table("pharos_personal_history"), ["person_id","cancer_type_code"], schema_pharos_ph, pharos_ph_comment,"Source_cd")
except Exception:
    import traceback
    _failure = traceback.format_exc()
    spark.createDataFrame([(str(globals().get('RUN_ID','bootstrap')), 25, _failure)], 'run_id string, cell int, error string').write.mode('append').saveAsTable('8_dev.default.pharos_pr13_failures')
    raise
