# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./0_config $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,ベクトル検索エンドポイントの作成
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()

client.create_endpoint(
    name=VECTOR_SEARCH_ENDPOINT_NAME,
    endpoint_type="STANDARD"  # または "STORAGE_OPTIMIZED"
)

# COMMAND ----------

# DBTITLE 1,インデックスの作成
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()

index = client.create_delta_sync_index(
  endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
  source_table_name=silver_table_name,
  index_name=index_name,
  pipeline_type="TRIGGERED",
  primary_key="Id",
  embedding_source_column="text",
  embedding_model_endpoint_name=embedding_endpoint
)

# COMMAND ----------


