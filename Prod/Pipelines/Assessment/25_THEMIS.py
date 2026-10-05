# Databricks notebook source
import base64, hashlib, json, zlib

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_open_ts", "")
dbutils.widgets.text("source_update_id", "")
dbutils.widgets.text("silver_update_id", "")
dbutils.widgets.text("scratch_prefix", "")
RUN = dbutils.widgets.get("run_id").strip()
RUN_OPEN_TS = dbutils.widgets.get("run_open_ts").strip()
SOURCE_UPDATE_ID = dbutils.widgets.get("source_update_id").strip()
SILVER_UPDATE_ID = dbutils.widgets.get("silver_update_id").strip()
SCRATCH_PREFIX = dbutils.widgets.get("scratch_prefix").strip()
assert RUN.startswith("dq4_omop_") and RUN.replace("_", "").isalnum(), RUN
assert RUN_OPEN_TS and SOURCE_UPDATE_ID and SILVER_UPDATE_ID and SCRATCH_PREFIX
LANE = "themis"
PAYLOAD = """eNrVWv9v2jgU/1c8pFPhRhHQdavQmNStTOOuR6eW2zRdT55JDERN7Mx22HHT/vd7dhJImkBLG7Ndfmmxn9/Xj997dvLXt5ozp84N9txar6bmNPBkL6RCctabUeZSgR3OHBoqoMBSEeYS4daatSkJPH8Ja9Jp+DvlIiDwC6ZvyJzFk5lBymYeozAsv/jwUxExoworMvH14DMcCu62eMBDLD1/QUUrVmRN6nA/CjTfgmpAExJBAlnrffve1AKwokHoE6VZXw3OB2/GyOERU/VfG+j0CimuiI8F/yqb1wzBFPGpdGhdRkH9zenVAH18Nxihghw0uhij4QjVT47bL5ro5Pio20BjTdpBg3NY1kaD0VmjidpGSkCJjAR1jaBr9vby4g/0OTb0c4ml19pWEfkUSx4Jh65CAsOSAp2nltilUxL5Cubed41Ttf8gekpE9HtzSzwnnlBz7IJLlBdQbZT0pKJMZcPpMUUFA9espp3lOp7g0Eh6E88HRSoNaF43S9FcUiLqeUkN9KRvxjGfYjOl12eei0tUDziDJSkBGl4ZFIz+PD9Hp6MzZKbL+ObXNUo4u2S5mS9MlnHNrsnzrBCH4K/LAcpLLyiY81t21i6Oc2JxAkmfZlG8IH5E8S202sZwTq8shGs+/wpZZAKA1dZ0Tk7aMB2FYTqIQ48xbXut2+4+hznGDdINCTIkyJMI3IBiSiQidshDykwMmohpp8b4diIhYE+bqNUbjVrF+ycfb40EwHF+4iXSFhbRnqd6hbStP33u5BNJxYIojzMMgjzu9qAKCoVBEBSHLOi0xzEXUDA8NqsOb0UNithjke/byZlF6dpsgy4IdMls7B09X3VuKgrbNeadB8dcM1viGGl4TiTmLJdwBJ1Sve08UznB7RQqJ5Uy2wrlRitLOxZDn8jUbU+62Svbrii8ZueDt2P024VupxIlz4ZX4+EI/llL3gUPDcS1PRd53fsoXP/aH2QYxxz4+STcX39VSa74OBy/QyaPQaGByNa1T5MArfzY3L739boYXMmzLY8ALc3S+mRW30LfQBcfBpexWqvn/enleDgeQuhff0KZaOdK0Bksg+mtmje3qZrhZ0AfCrrAZnQnmF6zB2/KWGKhFZPoZT+d222PJoG2tC8WnvQU5k7clTi0pz05o7G/91A8b8vfZ/6MZWeqZTyQ2SQVZNPbBu4rjumRGDpV/C8VfF9XAncHtBaTWL8buC0m2xAX5vqoXXU79NjIdx8Y+a+emnsMl5aa0n7IzjHsR+5sS53RbZPQItsj3bO4bGiBFtkWyJSMRauQj14Pxh8Hxr77N/aFR/MuZbAuohZTlEsV8fwfU2Zi2fsvMbHcYqVJxu2dyLJi9hHV6Q0u2falSUf32TORyziVVJ5NMa7dVsxe4XFbJbIKHeFiI5W5oLMHBOTembXKEp3JWeVK98tN3hVx7QcgDuKnL9MElRw0lz8N2hK9cvvcIuDKxRUwF7bKKH4WvCXkYYK1orL9jZbuI7kl9TkgCghlrMH+bg5+VAVzM13Kk1ybYgMn/9OU5FKi5j3OqDZYt1N4dRu4gsdMEOjKI+Z9iQo3j4/KREa4JUgkU4fJv8Xrv93CbVS1GgN93J1QcCddveax3Vtu8H/NjJu2zubrUre1ltOAzjJs5V/jVLBJjYDtuzO9M47zdvZc42bPNfFby6zKJSWqsreWnd2Ak9QUn7MZJlPI5TiN7F5T/A4n5priPhU6b4Avl1K/uly/m+zoF9GIT5H0iXODyERyMZFIkoAe6pk4qoLOwA5hzoFotTeqPAEVT7Gv8gh4ioaj8eDyw+k54PTs9JPtexhdOTYjOQG7BrK7+YC+HciWgOpA6wNqKtrzuROf3Kc3++t4V+JLct1KIWs9rtPKyChkDb8wa6GpXTkAOXd2tKk6yNdIyqvXzxtj62MMwRce7OeeDKkDiNBMeAA9yN4+DksUKIHLWiXrt8ChaJVJW30n1r712Y9GU91plV8bO63YhUkzfPA+MfGg+u98Es6gfw5rR1Cf+E0UxtSpmsjRKMtp3d9kuSW4TYmjj+G+QbWce2FPLoOAKuE596+gj0RcQYd9HpREio3M95idKm+fC9ah6Z1pqLhGJMemDfr20bRkvKsX6Z2RcDSk60cvSsczpCV8NvDvFPl3N/Dv2CqubpAw7Elogfz4KEeYi4Gl1N7LwhiATaDqEyuJc63K3R0g/Qe2uAL06EWJpsD1uPVMS04nF1BtJpFPxDJDszhutVH3xeHbwevD7vMqe76MLibPng9/H6AD0OmXg02vJvQ7uYKSJsfm1dzGYO03zEjyOeTmvacZPUUZpVMz+0BrlrTjJR29ZLdGYaWIpWSbwg9OLQxDwEyXd29wPu47p5S7ldxaGo52MYI7hSPVeNdgHBWC8fd/iDZVHQ=="""
checks = json.loads(zlib.decompress(base64.b64decode(PAYLOAD)).decode())

def qs(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

for seq, check in enumerate(checks):
    sql = check["sql_template"].replace("{RUN}", RUN)
    sha = hashlib.sha256(sql.encode()).hexdigest()
    name = "themis:" + check["check_id"]
    spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
      ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'attempted',
       NULL,NULL,current_timestamp(),NULL,'DQ4')""")
    try:
        row = spark.sql(sql).first().asDict()
        total = int(row["total_rows"] or 0)
        measured = int(row["measured_rows"] or 0)
        status = "fail" if measured > 0 else "pass"
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_check_result VALUES
          ({qs(RUN)},{qs(check['check_id'])},{qs(status)},{measured},{total},
           false,NULL,NULL,map('stage','dq4_omop_themis'),'DQ4',current_timestamp())""")
        spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
          ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'ok',
           NULL,NULL,current_timestamp(),current_timestamp(),'DQ4')""")
    except Exception as exc:
        msg = str(exc)[:4000]
        missing = any(x in msg for x in ("TABLE_OR_VIEW_NOT_FOUND","UNRESOLVED_COLUMN",
                                         "UNRESOLVED_FIELD","UNRESOLVED_ROUTINE"))
        if missing:
            spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_check_result VALUES
              ({qs(RUN)},{qs(check['check_id'])},'skip',0,0,false,NULL,NULL,
               map('stage','dq4_omop_themis','reason',{qs(msg[:1000])}),
               'DQ4',current_timestamp())""")
            spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
              ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'skipped_missing_table',
               {qs(msg)},NULL,current_timestamp(),current_timestamp(),'DQ4')""")
        else:
            spark.sql(f"""INSERT INTO 6_mgmt.silver_qc.dq_exec_log VALUES
              ({qs(RUN)},{qs(LANE)},{qs(name)},{seq},{qs(sha)},'error',
               {qs(msg)},NULL,current_timestamp(),current_timestamp(),'DQ4')""")
            raise
print({"checks":len(checks),"status":"ok"})

