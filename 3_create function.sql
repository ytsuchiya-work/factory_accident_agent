-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### プロンプトの例
-- MAGIC あなたは工場の作業員に対して、事故防止のアドバイスを行う専門家です。
-- MAGIC 作業員がこれから作業する装置を調べ、該当の装置で発生した過去の事故情報を元に注意点を案内してください。

-- COMMAND ----------

-- MAGIC %run ./0_config $reset_all_data=false

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_incident_report_by_device(given_device STRING)
    RETURNS TABLE(text STRING)
    COMMENT "Return incident report by given device category"
    RETURN SELECT text from accident_report_silver
    where incident_device = given_device
    limit 5


-- COMMAND ----------

select * from get_incident_report_by_device('固定屋根式地上タンク')

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_current_user()
    RETURNS STRING
    COMMENT "Return current_user_id"
    RETURN session_user()

-- COMMAND ----------

select get_current_user()

-- COMMAND ----------

create or replace table task_plan
(user_id string,
 working_device string);

insert into task_plan
values
  ('yusuke.tsuchiya@databricks.com','固定屋根式地上タンク'),
  ('anthony.cleg@databricks.com','ミキサー'),
  ('david.harland@databricks.com','常圧蒸留装置');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_user_task(given_user_id STRING)
    RETURNS TABLE(device STRING)
    COMMENT "Return the device the given user is going to work for."
    RETURN SELECT working_device from task_plan
    limit 1

-- COMMAND ----------

select * from get_user_task((select get_current_user()))

-- COMMAND ----------


