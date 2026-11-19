# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1.2: Query Usage and Cost Attribution
# MAGIC
# MAGIC ## Overview
# MAGIC In Lab 1.1, you sent tagged requests to your Model Service. This lab shows you where that evidence lives and how to query it to answer "What happened?" and "What did it cost?"
# MAGIC
# MAGIC **Learning objective:** Query `system.ai_gateway.usage` and `system.billing.usage` to understand who called what, how many tokens were consumed, and what the cost impact was.
# MAGIC
# MAGIC **Important:** These are separate system tables with different schemas, refresh behavior, and purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Evidence Sources
# MAGIC
# MAGIC **`system.ai_gateway.usage`** (operational telemetry — AVAILABLE NOW):
# MAGIC - Logs every request routed through Unity Gateway
# MAGIC - Records: endpoint, tokens, latency, request tags, caller, status code, destination model
# MAGIC - Refreshes: ingestion-dependent (records may become queryable shortly after requests are made, but availability is not guaranteed immediate)
# MAGIC - Purpose: operational analytics, debugging, real-time tagging verification
# MAGIC - **This is what you'll query in Steps 3 & 4 below**
# MAGIC
# MAGIC **`system.billing.usage`** (cost attribution — AVAILABLE LATER):
# MAGIC - Records Databricks-hosted model consumption for billing
# MAGIC - Source of truth for actual costs
# MAGIC - Refreshes: typically within 12 hours (may be longer for new workspaces)
# MAGIC - **⏱️ Significant lag: requests made during this workshop may not appear until later**
# MAGIC - Purpose: cost attribution, finance reporting, monthly billing
# MAGIC
# MAGIC **`system.ai_gateway.external_model_spend`** (estimate only):
# MAGIC - Hourly estimates of external model costs (e.g., third-party APIs)
# MAGIC - Informational; provider invoice is the actual source of truth
# MAGIC - Purpose: cost forecasting, spend tracking
# MAGIC
# MAGIC Each table serves a different purpose. Don't assume one contains all information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import re

w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = user_name.split('@')[0]

# Sanitize user_id
invalid_chars = r'[\s\.\/\x00-\x1f]'
if re.search(invalid_chars, user_id):
    user_id = re.sub(invalid_chars, '_', user_id)

catalog_name = user_id
schema_name = "sales"
service_name = f"{user_id}_lab_model_service"
model_service_fqn = f"{catalog_name}.{schema_name}.{service_name}"

spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"User: {user_name}")
print(f"Model Service FQN: {model_service_fqn}")
print(f"\nQuerying system tables for requests to: {service_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Inspect system.ai_gateway.usage Schema
# MAGIC
# MAGIC First, let's see what fields are actually available in this workspace.

# COMMAND ----------

# DBTITLE 1,Inspect the schema of system.ai_gateway.usage
try:
    spark.sql("DESCRIBE TABLE system.ai_gateway.usage").display()
    print("\n✅ system.ai_gateway.usage is available in this workspace")
except Exception as e:
    print(f"⚠️  system.ai_gateway.usage may not be available: {e}")
    print("   This may be due to:")
    print("   - Workspace not having Unity Catalog fully enabled")
    print("   - Regional availability (check workspace configuration)")
    print("   - Insufficient permissions (ask workspace admin)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Recent Requests (After Lab 1.1)
# MAGIC
# MAGIC **Note:** After you send a real request in Lab 1.1, wait for ingestion (typically shortly after, but availability is not guaranteed immediate). Then run this query to find it.
# MAGIC
# MAGIC If the table exists, this will show you requests routed through your Model Service.

# COMMAND ----------

# DBTITLE 1,Find recent requests to your Model Service
try:
    result_df = spark.sql(f"""
    SELECT
      request_id,
      event_time,
      endpoint_name,
      input_tokens,
      output_tokens,
      input_tokens + output_tokens AS total_tokens,
      request_tags
    FROM system.ai_gateway.usage
    WHERE endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
    ORDER BY event_time DESC
    LIMIT 20
    """)

    result_count = result_df.count()
    if result_count > 0:
        result_df.display()
        print(f"\n✅ Query succeeded — found {result_count} request(s). Look at request_tags to verify your metadata was recorded.")
    else:
        print("\n⏱️  No rows found yet — your request may still be in the ingestion pipeline.")
        print("   Ingestion lag can range from minutes to ~45 minutes. Proceed to the next lab and revisit later.")

except Exception as e:
    print(f"⚠️  Query failed: {e}")
    print("\n   Possible reasons:")
    print("   - system.ai_gateway.usage is not available in this workspace")
    print("   - No requests have been sent yet (run Lab 1.1 first)")
    print("   - Ingestion delay (records may take time to become queryable; wait and retry)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Aggregate Usage by Request Tags
# MAGIC
# MAGIC Once requests are logged, you can slice by team, project, or cost_center.

# COMMAND ----------

# DBTITLE 1,Total tokens consumed by team
try:
    result_df = spark.sql(f"""
    SELECT
      request_tags['team'] AS team,
      COUNT(*) AS request_count,
      SUM(input_tokens) AS total_input_tokens,
      SUM(output_tokens) AS total_output_tokens,
      SUM(input_tokens + output_tokens) AS total_tokens,
      ROUND(AVG(input_tokens + output_tokens), 2) AS avg_tokens_per_request
    FROM system.ai_gateway.usage
    WHERE endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
    GROUP BY 1
    ORDER BY total_tokens DESC
    """)

    result_count = result_df.count()
    if result_count > 0:
        result_df.display()
    else:
        print("⏱️  No aggregated data found yet. This is expected if your requests haven't been ingested.")
        print("   Ingestion lag can range from minutes to ~45 minutes. Revisit this query later.")

except Exception as e:
    print(f"Note: If this query fails, system.ai_gateway.usage may not be available or no data exists yet.")

# COMMAND ----------

# DBTITLE 1,Total tokens consumed by project
try:
    result_df = spark.sql(f"""
    SELECT
      request_tags['project'] AS project,
      request_tags['team'] AS team,
      COUNT(*) AS request_count,
      SUM(input_tokens + output_tokens) AS total_tokens
    FROM system.ai_gateway.usage
    WHERE endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
    GROUP BY 1, 2
    ORDER BY total_tokens DESC
    """)

    result_count = result_df.count()
    if result_count > 0:
        result_df.display()
    else:
        print("⏱️  No aggregated data found yet. This is expected if your requests haven't been ingested.")
        print("   Ingestion lag can range from minutes to ~45 minutes. Revisit this query later.")

except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: What's Next? Billing Data (End of Workshop)
# MAGIC
# MAGIC **Important:** Billing data typically refreshes within 12 hours (may be longer for new workspaces), so requests from this lab won't appear immediately.
# MAGIC
# MAGIC After the workshop ends:
# MAGIC - Return to this lab
# MAGIC - Run the queries below to see how your Model Service requests appear in `system.billing.usage`
# MAGIC - Compare request tags (operational, queryable shortly after requests) vs. billing aggregations (12-hour lag typical)
# MAGIC
# MAGIC For now, focus on what you've demonstrated:
# MAGIC - ✅ Sent a tagged request (Lab 1.1)
# MAGIC - ✅ Verified it routed through your Model Service (Lab 1.1 Step 6)
# MAGIC - ✅ Queried request-level detail with tags (Lab 1.2 Steps 3 & 4)
# MAGIC
# MAGIC Cost attribution is complete at the operational level. Billing follows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Optional] Revisit After Workshop: Query system.billing.usage
# MAGIC
# MAGIC For Databricks-hosted models, the source of truth for cost is `system.billing.usage`.
# MAGIC
# MAGIC **Key differences:**
# MAGIC - `system.ai_gateway.usage` = request-level detail (seconds refresh, includes request tags)
# MAGIC - `system.billing.usage` = billing aggregation (hourly/daily refresh, uses service tags)

# COMMAND ----------

# DBTITLE 1,[End of Workshop] Inspect system.billing.usage schema
try:
    spark.sql("DESCRIBE TABLE system.billing.usage").display()
    print("\n✅ system.billing.usage is available")

except Exception as e:
    error_msg = str(e)
    if 'PERMISSION_DENIED' in error_msg or 'permission' in error_msg.lower():
        print(f"⚠️  Permission denied — your account may not have access to system.billing.usage.")
        print("   Contact your workspace admin to grant USE SCHEMA on system.billing.")
    else:
        print(f"Note: system.billing.usage may not be available: {e}")

# COMMAND ----------

# DBTITLE 1,[End of Workshop] Query Databricks model consumption
try:
    spark.sql(f"""
    SELECT
      service_name,
      custom_tags,
      sku,
      usage_quantity,
      usage_unit,
      billing_currency
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAY
      AND billing_origin_product = 'MODEL_SERVING'
      AND usage_unit = 'DBU'
    ORDER BY usage_date DESC
    LIMIT 20
    """).display()

    print("\n📊 Billing data: Shows Model Serving costs in DBU (Databricks Units)")
    print("   custom_tags contains team and other cost-attribution dimensions")
    print("   To filter for your specific Model Service, look in the custom_tags column")
    print("   Billing typically appears within 12 hours. This query will return data after the workshop.")

except Exception as e:
    error_msg = str(e)
    if 'PERMISSION_DENIED' in error_msg or 'permission' in error_msg.lower():
        print(f"⚠️  Permission denied — your account may not have access to system.billing.usage.")
        print("   Contact your workspace admin to grant USE SCHEMA on system.billing.")
    else:
        print(f"Note: Billing query returned no data. Reasons:")
        print("   - Billing data has not refreshed yet (typical lag: 12 hours, may be longer for new workspaces)")
        print("   - The Model Service may route to external models (check system.ai_gateway.external_model_spend)")
        print("   - No requests have been billed yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence Sources Summary
# MAGIC
# MAGIC | Question | Table | Refresh | When Available | Notes |
# MAGIC |----------|-------|---------|---|-------|
# MAGIC | Which AI Gateway requests occurred? Token counts, latency, status, requester, tags? | `system.ai_gateway.usage` | Ingestion-dependent | Shortly after requests (not guaranteed immediate) | Request-level detail, includes request_tags; primary table for per-request analysis |
# MAGIC | What Databricks-billed usage/cost was incurred? | `system.billing.usage` (join with `system.billing.list_prices` to estimate USD) | Typically within 12 hours | After workshop | Uses custom_tags for attribution; longer delays possible for new workspaces |
# MAGIC | What estimated external-model spend was incurred? | `system.ai_gateway.external_model_spend` | Hourly | End of workshop (if applicable) | Estimates only; join with provider invoices for actual charges |
# MAGIC | Who changed Gateway configuration or accessed an AI service, and when? | `system.access.audit` | Audit-event ingestion | Varies | Audit trail for configuration changes and access events; not per-request tracking |
# MAGIC
# MAGIC **Key timing insight:** You see attribution in `system.ai_gateway.usage` shortly after requests are made. Billing appears later in `system.billing.usage` (typically within 12 hours). Don't assume one table replaces another—they answer different questions at different times.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Request Tags vs. Service Tags**: Lab 1.1 used request tags. Why would you configure service tags on the Model Service instead?
# MAGIC    - *Expected: Service tags apply to all callers, simplifying attribution for shared services; request tags allow per-call variation*
# MAGIC
# MAGIC 2. **Billing Lag**: If `system.ai_gateway.usage` updates in seconds but `system.billing.usage` updates hourly, what does that mean for real-time cost tracking?
# MAGIC    - *Expected: Use AI Gateway usage for real-time monitoring, billing for actual cost accrual and month-end reporting*
# MAGIC
# MAGIC 3. **Tag Design**: How would you structure your request tags so that finance can easily chargeback to cost centers?
# MAGIC    - *Expected: Include cost_center, department, project, environment so queries can GROUP BY these fields*
# MAGIC
# MAGIC 4. **Evidence Mismatch**: If you see high token usage in `system.ai_gateway.usage` but no entry in `system.billing.usage`, what could explain it?
# MAGIC    - *Expected: External model (billing on provider invoice, not Databricks), billing lag, or service not configured for billing*

# COMMAND ----------

# DBTITLE 1,Lab 1.2 Summary
print(f"""
✅ Lab 1.2 Overview Complete

Evidence sources for your Model Service: {model_service_fqn}

Key insights:
- system.ai_gateway.usage = queryable shortly after requests (ingestion-dependent), request-level detail with request_tags
- system.billing.usage = delayed (typically within 12 hours), billing aggregation with custom_tags
- Attribution happens at request time (Step 3 & 4), billing appears later

Next: Lab 1.3 — Set up rate limits and budgets to control spend.
""")