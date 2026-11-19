# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 2.2: Audit Trail Analysis at Scale
# MAGIC
# MAGIC ## Overview
# MAGIC After granting permissions (Lab 2.1), access control events are logged to `system.access.audit`. This lab teaches how to query audit logs to answer compliance, security, and troubleshooting questions.
# MAGIC
# MAGIC **Learning outcome:** Use audit logs to investigate who initiated access, when, and what the response was.
# MAGIC
# MAGIC ⚠️ **Important:** `system.access.audit` is a Public Preview system table. Field names and availability are workspace-dependent. Always inspect the schema first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: What system.access.audit Logs
# MAGIC
# MAGIC `system.access.audit` records **documented audit events**:
# MAGIC - Includes both successful and failed access/control-plane operations
# MAGIC - Event coverage is service- and action-specific (not comprehensive for all internal checks)
# MAGIC - Metadata: Who (user_identity), what (action_name), when (event_time), response status
# MAGIC
# MAGIC What to use for specific questions:
# MAGIC - **Access control events** → `system.access.audit`
# MAGIC - **All Model Service requests and tokens** → `system.ai_gateway.usage`
# MAGIC - **Payloads and interaction details** → Configured inference tables or traces
# MAGIC
# MAGIC Use cases:
# MAGIC 1. **Compliance**: "What access control events occurred?"
# MAGIC 2. **Security**: "Did anyone encounter authorization failures? Why?"
# MAGIC 3. **Troubleshooting**: "What was the response to user X's request?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = user_name.split('@')[0]

# Sanitize user_id: replace invalid characters (spaces, periods, slashes, control chars) with underscores
import re
invalid_chars = r'[\s\.\/\x00-\x1f]'
if re.search(invalid_chars, user_id):
    user_id = re.sub(invalid_chars, '_', user_id)

catalog_name = user_id
spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql("USE SCHEMA sales")

print(f"User: {user_name}")
print(f"Catalog: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Inspect system.access.audit Schema
# MAGIC
# MAGIC First, let's see what fields are available in this workspace.

# COMMAND ----------

# DBTITLE 1,Inspect system.access.audit schema
try:
    spark.sql("DESCRIBE TABLE system.access.audit").display()
    print("\n✅ system.access.audit is available in this workspace")
    print("   Key fields:")
    print("   - event_time: When the event occurred (TIMESTAMP)")
    print("   - event_date: Partitioning date (DATE) — use for efficient filtering")
    print("   - user_identity: STRUCT containing email, subject_name, etc.")
    print("   - service_name: Name of the service that generated the event")
    print("   - action_name: Action name (e.g., EXECUTE)")
    print("   - response: STRUCT with status_code, error_message, result")
    print("   - request_params: MAP containing event-specific parameters")

except Exception as e:
    print(f"⚠️  system.access.audit is not available: {e}")
    print("\n   This is expected if:")
    print("   - Your workspace doesn't have Unity Catalog fully enabled")
    print("   - Access audit is not available in your region")
    print("   - You lack permissions to query system tables")
    print("\n   Note: The queries below assume the table exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Access Patterns
# MAGIC
# MAGIC **Note:** These queries use `system.access.audit`. If the table is not available, they will show an error message.

# COMMAND ----------

# DBTITLE 1,Access attempts by user (who's most active?)
try:
    spark.sql("""
    SELECT
      user_identity.email AS principal,
      COUNT(*) AS events,
      COUNT(DISTINCT service_name) AS services,
      MAX(event_time) AS most_recent_event
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAY
    GROUP BY 1
    ORDER BY events DESC
    LIMIT 20
    """).display()

except Exception as e:
    print(f"Note: Query failed: {e}")
    print("   system.access.audit may not be available in this workspace.")

# COMMAND ----------

# DBTITLE 1,Events with non-2xx status codes (troubleshooting)
try:
    spark.sql("""
    SELECT
      user_identity.email AS principal,
      service_name,
      action_name,
      response.status_code AS status_code,
      response.error_message AS error_message,
      COUNT(*) AS event_count,
      MIN(event_time) AS first_event,
      MAX(event_time) AS latest_event
    FROM system.access.audit
    WHERE response.status_code >= 400
      AND event_date >= CURRENT_DATE() - INTERVAL 7 DAY
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY event_count DESC
    LIMIT 20
    """).display()

except Exception as e:
    print(f"Note: Query failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Evidence Sources Summary
# MAGIC
# MAGIC System.access.audit logs **permission checks only**. For a complete picture of your Model Service calls, use these tables together:

# COMMAND ----------

# MAGIC %md
# MAGIC | Question | Table | Notes |
# MAGIC |----------|-------|-------|
# MAGIC | What audit/control-plane events occurred? | `system.access.audit` | Supported events only; event coverage is service- and action-specific |
# MAGIC | How many Model Service requests? | `system.ai_gateway.usage` | Request-level telemetry, tokens, latency, status, requester |
# MAGIC | What was sent and what was the response? | `system.ai_gateway.usage` + inference tables | Usage has request metadata; inference tables have payloads |
# MAGIC | What was the cost? | `system.billing.usage` | Billing data; Databricks models only |
# MAGIC
# MAGIC These are separate evidence sources with different coverage. Combine them to build the complete picture.

# COMMAND ----------

# DBTITLE 1,Events with 2xx status codes (last 24 hours)
try:
    spark.sql("""
    SELECT
      user_identity.email AS principal,
      service_name,
      action_name,
      COUNT(*) AS event_count,
      MIN(event_time) AS first_event,
      MAX(event_time) AS most_recent_event
    FROM system.access.audit
    WHERE response.status_code >= 200 AND response.status_code < 300
      AND event_date >= CURRENT_DATE() - INTERVAL 1 DAY
    GROUP BY 1, 2, 3
    ORDER BY event_count DESC
    LIMIT 50
    """).display()

except Exception as e:
    print(f"Note: Query failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Troubleshooting a Specific User
# MAGIC
# MAGIC Why would you query audit logs? Often to troubleshoot: "User X says they can't call the Model Service. Why?"

# COMMAND ----------

# DBTITLE 1,Troubleshoot events for the current user
print(f"Querying audit events for: {user_name}\n")

try:
    result = spark.sql(f"""
    SELECT
      event_time,
      service_name,
      action_name,
      response.status_code AS status_code,
      response.error_message AS error_message,
      response.result AS result
    FROM system.access.audit
    WHERE user_identity.email = '{user_name}'
      AND event_date >= CURRENT_DATE() - INTERVAL 7 DAY
    ORDER BY event_time DESC
    LIMIT 20
    """).display()

    print(f"\n✅ Audit events for {user_name} (generated in Lab 2.1 and subsequent labs)")

except Exception as e:
    print(f"Note: Query failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Audit Log Insights
# MAGIC
# MAGIC Use audit logs to understand usage patterns and troubleshoot access issues.

# COMMAND ----------

# DBTITLE 1,Daily summary: Audit event trends
try:
    spark.sql("""
    SELECT
      event_date AS date,
      COUNT(*) AS total_events,
      COUNT(DISTINCT user_identity.email) AS unique_users,
      COUNT(DISTINCT service_name) AS services,
      SUM(CASE WHEN response.status_code >= 400 THEN 1 ELSE 0 END) AS error_events
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAY
    GROUP BY 1
    ORDER BY event_date DESC
    """).display()

except Exception as e:
    print(f"Note: Query failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Insight: Audit Logs as Governance Evidence
# MAGIC
# MAGIC **`system.access.audit` is a source of recorded events, not a universal ledger:**
# MAGIC 1. **Documented events are logged** (audit and control-plane events per service)
# MAGIC 2. **Logs are immutable** (system tables are append-only)
# MAGIC 3. **Event coverage is service- and action-specific** (not all internal operations)
# MAGIC
# MAGIC **Important distinctions:**
# MAGIC - `system.access.audit`: Recorded audit events and control-plane operations
# MAGIC - `system.ai_gateway.usage`: All Model Service requests, tokens, latency, status, requester
# MAGIC - Configured inference tables: Payloads and interaction details
# MAGIC
# MAGIC Combine these to build a complete governance picture:
# MAGIC - **Access events** → system.access.audit
# MAGIC - **Request volume and cost** → system.ai_gateway.usage
# MAGIC - **Payload audit** → Inference tables or traces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Evidence Completeness**: `system.access.audit` logs documented events. What other tables do you need for a complete governance record?
# MAGIC 2. **Error Investigation**: If you see error_code 403 in system.access.audit, what questions would you ask to root-cause it?
# MAGIC 3. **Data Retention**: How long should you keep audit logs? How does that depend on your compliance requirements?
# MAGIC 4. **Multi-Table Queries**: To investigate "User X made 100 requests but 50 were blocked", which tables would you join?

# COMMAND ----------

# DBTITLE 1,Lab 2.2 Summary
print(f"""
✅ Lab 2.2 Complete

What you accomplished:
1. Inspected system.access.audit schema and key fields
2. Queried audit events by user, service, and action
3. Investigated events with error status codes
4. Understood audit log coverage vs. other evidence sources

Key insight: system.access.audit records supported audit events (immutable, append-only).
Combined with system.ai_gateway.usage (request telemetry) and configured inference tables (payloads),
you build complete governance observability.

Next: Lab 2.3 — Implement request filtering with service policies.
""")