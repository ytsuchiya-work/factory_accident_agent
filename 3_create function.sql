-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### プロンプトの例
-- MAGIC あなたは工場の作業員に対して、事故防止のアドバイスを行う専門家です。
-- MAGIC 作業員がこれから作業する装置を調べ、該当の装置で発生した過去の事故情報を元に注意点を案内してください。

-- COMMAND ----------

-- MAGIC %run ./0_config $reset_all_data=false

-- COMMAND ----------

-- 以下の関数のカタログとスキーマを適宜変更してください

CREATE OR REPLACE FUNCTION ytsuchiya.factory_accident.get_incident_report_by_device(given_device STRING)
    RETURNS TABLE(text STRING)
    COMMENT "Return incident report by given device category"
    RETURN SELECT text from ytsuchiya.factory_accident.accident_report_silver
    where incident_device = given_device
    limit 5


-- COMMAND ----------

-- 以下のクエリのカタログとスキーマを適宜変更してください

select * from ytsuchiya.factory_accident.get_incident_report_by_device('固定屋根式地上タンク')

-- COMMAND ----------

-- 以下の関数のカタログとスキーマを適宜変更してください

CREATE OR REPLACE FUNCTION ytsuchiya.factory_accident.get_current_user()
    RETURNS STRING
    COMMENT "Return current_user_id"
    RETURN session_user()

-- COMMAND ----------

-- 以下のクエリのカタログとスキーマを適宜変更してください

select ytsuchiya.factory_accident.get_current_user()

-- COMMAND ----------

-- 以下のクエリのカタログとスキーマを適宜変更してください

create or replace table ytsuchiya.factory_accident.task_plan
(user_id string,
 working_device string);

insert into ytsuchiya.factory_accident.task_plan
values
  ('yusuke.tsuchiya@databricks.com','固定屋根式地上タンク'),
  ('anthony.cleg@databricks.com','ミキサー'),
  ('david.harland@databricks.com','常圧蒸留装置');

-- COMMAND ----------

-- 以下の関数のカタログとスキーマを適宜変更してください

CREATE OR REPLACE FUNCTION ytsuchiya.factory_accident.get_user_task(given_user_id STRING)
    RETURNS TABLE(device STRING)
    COMMENT "Return the device the given user is going to work for."
    RETURN SELECT working_device from ytsuchiya.factory_accident.task_plan
    limit 1

-- COMMAND ----------

-- 以下のクエリのカタログとスキーマを適宜変更してください

select * from ytsuchiya.factory_accident.get_user_task((select ytsuchiya.factory_accident.get_current_user()))

-- COMMAND ----------


