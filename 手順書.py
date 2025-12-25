# Databricks notebook source
# MAGIC %md
# MAGIC ## はじめに
# MAGIC Playgroundで選択するモデルは、**ツールが対応していれば**何でも良いです。
# MAGIC
# MAGIC ![](./img/tool_model.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. RAGの作成
# MAGIC
# MAGIC Playground でツールとして`Vector Search` -> `{catalog}.{schema}.accident_report_silver_index` を登録した上で、下記のプロンプトを指定してください。
# MAGIC
# MAGIC ![](./img/vs.png)
# MAGIC
# MAGIC #### システムプロンプト
# MAGIC あなたは工場における事故事例の専門家です。
# MAGIC 与えられた質問に対して、過去の事故事例を具体的に参照しながら回答してください。
# MAGIC 必要に応じて、ツールを利用して過去の事故事例を取得してください。その際、質問と関係ない事例が含まれていた場合、その事例は使わないでください。
# MAGIC
# MAGIC #### プロンプト
# MAGIC 配管の不具合を最新順に教えて
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 作業の注意点を教えてくれるエージェント
# MAGIC Playground でツールとして、すべてのUC関数 `{catalog}.{schema}.*`を登録した上で、下記のプロンプトを指定してください。
# MAGIC
# MAGIC ![](./img/function.png)
# MAGIC
# MAGIC #### システムプロンプト
# MAGIC あなたは工場の作業員に対して、事故防止のアドバイスを行う専門家でもあります。
# MAGIC 作業員がこれから作業する装置を調べ、該当の装置で発生した過去の事故情報を元に注意点を案内してください。
# MAGIC
# MAGIC #### プロンプト
# MAGIC これから作業します。

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./img/setting.png)

# COMMAND ----------


