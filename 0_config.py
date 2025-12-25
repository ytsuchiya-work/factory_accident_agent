# Databricks notebook source
# DBTITLE 1,Data Prep用
catalog = "ytsuchiya"
schema = "factory_accident"

silver_table_name = f"{catalog}.{schema}.accident_report_silver"
index_name = f"{catalog}.{schema}.accident_report_silver_index"

# ソースのファイルが存在するdirectory
pdf_folder_name = "source"
pdf_dir = f"/Volumes/{catalog}/{schema}/{pdf_folder_name}"

curate_llm_endpoint_name = "databricks-gpt-5"
embedding_endpoint = "databricks-gte-large-en"

VECTOR_SEARCH_ENDPOINT_NAME = "vs-endpoint"

MODEL_NAME = "factory_accident_chatbot_demo"
MODEL_NAME_FQN = f"{catalog}.{schema}.{MODEL_NAME}"
agent_endpoint_name = f'agents_{catalog}-{schema}-{MODEL_NAME}'[:60]
