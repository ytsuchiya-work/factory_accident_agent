# Databricks notebook source
# MAGIC %md
# MAGIC ClusterはサーバレスでOK
# MAGIC
# MAGIC [この辺](https://qiita.com/ryutarom128/items/6e5d36efb136f9595f07)を参考にしてpypdfを利用
# MAGIC
# MAGIC 公式のマニュアルは[これ](https://pypdf.readthedocs.io/en/stable/)

# COMMAND ----------

# MAGIC %pip install pypdf

# COMMAND ----------

# MAGIC %run ./0_config $reset_all_data=false

# COMMAND ----------

from pypdf import PdfReader
import os
from pyspark.sql import Row

data = []

# ディレクトリ内の全てのPDFファイルを読み込む
for filename in os.listdir(pdf_dir):
    extracted_text = ""
    if filename.endswith(".pdf"):
        pdf_path = os.path.join(pdf_dir, filename)
        
        # PDFファイルの読み込み
        reader = PdfReader(pdf_path)
        
        # ページ数の取得
        number_of_pages = len(reader.pages)
        
        for i in range(number_of_pages):
            # ページの取得。
            page = reader.pages[i]
            extracted_text += page.extract_text()
        
        data.append(Row(path=pdf_path, text=extracted_text))

df = spark.createDataFrame(data)
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.accident_report_bronze_pdf")

# COMMAND ----------

df = spark.read.table(f"{catalog}.{schema}.accident_report_bronze_pdf")
display(df)

# COMMAND ----------

# PDFから抽出したテキストの整形処理をAIで実施。ヘッダーやフッター、不要な改行などを除去し、読みやすいテキストに変換。

prompt = "'Format the following text which is orginally extracted from pdf files. It may contain some unnesessary information like header and footer, and unexpected line break. Not include additonal comment like [Here is the formatted text:]'"
sql_text = f"""create or replace table {catalog}.{schema}.accident_report_bronze_pdf_curated as select
*,
ai_query('{curate_llm_endpoint_name}', concat({prompt},text)) as curated_text
from {catalog}.{schema}.accident_report_bronze_pdf
"""

spark.sql(sql_text)
display(spark.read.table(f"{catalog}.{schema}.accident_report_bronze_pdf_curated"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entityの抽出
# MAGIC ai_queryを利用して、テキストからエンティティを抽出

# COMMAND ----------

prompt = """
'The following document is an incident report for a specific incident.
Please extract the information as much as accurate and detailed. You can join or merge some columns/attributes if needed.
Include the following JSON only in the response. Not include any other comment. 
response example:
{
  "事例番号": "00427",
  "発生年月日": 【事象の発生した年月日。例: "2023/01/01"】,
  "タイトル": 【事象のタイトル。例: "燃料油移送配管の外面腐食による開口、重油流出"】,
  "事故概要": 【事象の概要。何が発生したかを簡潔に記述。】,
  "事故詳細"【事象の詳細を記述。発生の経緯や発生したこと、原因など、事象に関することを全て記述】,
  "事故への対策": 【同様の事故を防ぐための対策を記述】,
  "事故の発生した装置の系統": 【事象が発生した装置の一番上位の系統を記載する。もしなければ空欄: 例)貯蔵系など】,
  "事故の発生した装置名": 【事象が発生した装置の一般名称を記載する。もしなければ空欄: 例)常圧蒸留装置、浮屋根式地上タンクなど】,
  "事故の発生した装置(機器番号)": 【事象が発生した装置の機器番号名を記載、もしなければ空欄: 例)C-P3A, ポンプB】,
  "事故の発生した機器(部位)": 【事象が発生した装置の機器番号名を記載、もしなければ空欄: 例)配管】,  
  "関連物質": 【配列。事象に関連する化学物質名を記載。複数ある場合は箇条書きにする。】,
  "被害状況(金額)": 【事象の被害金額(円)を数字で記載: 例: 1,000,000】
}
###############'
"""

# COMMAND ----------

sql_text = f"select path, text, ai_query('{curate_llm_endpoint_name}',concat({prompt},text)) as entity from {catalog}.{schema}.accident_report_bronze_pdf_curated" 
df = spark.sql(sql_text)
display(df)

# COMMAND ----------

spark.sql(f"drop table if exists {catalog}.{schema}.accident_report_bronze_entity_extracted")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn("entity", regexp_replace(col("entity"), r"`{3}", ""))
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.accident_report_bronze_entity_extracted")


# COMMAND ----------

sql_text = f"""
select 
row_number() OVER (ORDER BY (SELECT NULL)) AS Id,
  entity:`発生年月日` as incident_date,
  entity:`事故の発生した装置の系統` as incident_device_category,
  entity:`事故の発生した装置名` as incident_device,
  entity:`事故の発生した機器(部位)` as incident_part,
  entity:`関連物質` as chemical_substance,
  entity:`被害状況` as accident_damage,
  text,
  entity,
  path
from {catalog}.{schema}.accident_report_bronze_entity_extracted
"""
df = spark.sql(sql_text)
display(df)

# COMMAND ----------

spark.sql(f"drop table if exists {catalog}.{schema}.accident_report_silver")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.accident_report_silver")

# COMMAND ----------

spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.accident_report_silver SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------


