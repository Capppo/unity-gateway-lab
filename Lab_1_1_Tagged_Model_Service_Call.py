# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 1.1: Send a Tagged Request to Your Model Service
# MAGIC
# MAGIC ## Overview
# MAGIC In this lab, you'll send a request to your Model Service with **request tags** attached. These tags are metadata (team, project, environment) that flow through Unity Gateway and are recorded in `system.ai_gateway.usage` for cost attribution.
# MAGIC
# MAGIC **Learning objective:** Understand how request tags enable cost attribution by seeing them flow through the request → system table → query chain.
# MAGIC
# MAGIC **Note:** This lab is designed to be safe to run in a workshop (no large token spend). We'll focus on the *mechanism* of tagging, not the scale of requests.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Request Tags vs. Service Tags
# MAGIC
# MAGIC Cost attribution uses two tag types:
# MAGIC
# MAGIC **Request Tags** (what this lab focuses on):
# MAGIC - Sent by the caller in the HTTP header: `Databricks-Ai-Gateway-Request-Tags`
# MAGIC - Passed as a JSON object: `{"team": "data-science", "project": "cost-governance-lab", ...}`
# MAGIC - Recorded in `system.ai_gateway.usage.request_tags` field
# MAGIC - Allow per-call attribution
# MAGIC
# MAGIC **Service Tags** (configured on the Model Service):
# MAGIC - Set when you create or update the Model Service
# MAGIC - Apply to *every* request routed through that service
# MAGIC - Recorded in `system.ai_gateway.usage` as service metadata
# MAGIC - Enable service-level attribution (e.g., "all GPT-4 calls are tagged as team=ai")
# MAGIC
# MAGIC For Databricks-hosted model billing, service tags are the primary attribution path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup Your Model Service FQN

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json
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

print(f"Current User: {user_name}")
print(f"Model Service FQN: {model_service_fqn}")
print(f"\nYou will use this FQN to invoke the service.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understand the Request Tag Header
# MAGIC
# MAGIC To attach request tags, include this HTTP header in your request:
# MAGIC
# MAGIC ```
# MAGIC Databricks-Ai-Gateway-Request-Tags: {"team": "data-science", "project": "cost-governance-lab", "cost_center": "ai-innovation"}
# MAGIC ```
# MAGIC
# MAGIC This JSON object is parsed and recorded in `system.ai_gateway.usage.request_tags` by Unity Gateway.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Sample Request Structure
# MAGIC
# MAGIC Here's what a real tagged request looks like (using Databricks SDK or REST API):

# COMMAND ----------

# This is a reference example. In a real call, you would:
# 1. Import the model serving client
# 2. Construct a request with your prompt
# 3. Include request tags in the header or parameters
# 4. Send the request

# Example request using the Responses API format (matches the actual call in Step 4)
example_headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer <token>",
    "Databricks-Ai-Gateway-Request-Tags": json.dumps({
        "team": "data-science",
        "project": "cost-governance-lab",
        "environment": "workshop",
        "cost_center": "ai-innovation"
    })
}

example_payload = {
    "model": model_service_fqn,
    "max_output_tokens": 256,
    "input": [
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "What are three ways to optimize AI spending in an enterprise?"
                }
            ]
        }
    ]
}

print("Example request structure (Responses API format):")
print("\nHeaders (request tags are sent as an HTTP header, NOT in the payload body):")
print(json.dumps(example_headers, indent=2))
print("\nPayload:")
print(json.dumps(example_payload, indent=2))
print("\n✅ Request tags are passed in the Databricks-Ai-Gateway-Request-Tags HTTP header.")
print("✅ Unity Gateway records these tags in system.ai_gateway.usage.request_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Send a Tagged Request to Your Model Service
# MAGIC
# MAGIC Use the Databricks SDK to send a request with request tags:

# COMMAND ----------

import requests

try:
    # Get workspace URL and token
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get().rstrip('/')
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    # Prepare request tags
    request_tags = {
        "team": "data-science",
        "project": "cost-governance-lab",
        "environment": "workshop",
        "cost_center": "ai-innovation"
    }

    # Correct endpoint for Model Service routing with request tags
    endpoint_url = f"{workspace_url}/ai-gateway/mlflow/v1/responses"

    # Prepare headers with request tags (critical for attribution)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "Databricks-Ai-Gateway-Request-Tags": json.dumps(request_tags)
    }

    # Prepare the request payload using Databricks responses API format
    payload = {
        "model": model_service_fqn,
        "max_output_tokens": 256,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "What are three ways to optimize AI spending in an enterprise?"
                    }
                ]
            }
        ]
    }

    print("Sending tagged request to Model Service...")
    print(f"Endpoint: {endpoint_url}")
    print(f"Model Service: {model_service_fqn}")
    print(f"Request tags: {json.dumps(request_tags, indent=2)}")

    # Send the request with explicit headers containing request tags
    response = requests.post(endpoint_url, json=payload, headers=headers, timeout=30)

    if response.status_code == 200:
        result = response.json()
        print("\n✅ Request succeeded!")

        # Extract model name
        if "model" in result:
            print(f"\nModel used: {result.get('model')}")

        # Extract response text from Responses API format
        response_text = None
        for item in result.get('output', []):
            if item.get('type') == 'message':
                for content in item.get('content', []):
                    if content.get('type') == 'output_text':
                        response_text = content['text']
        if response_text:
            print(f"\nModel response text:\n{response_text}")
        else:
            print("\nModel response text: (could not extract from response)")

        # Show token usage
        usage = result.get('usage', {})
        if usage:
            print(f"\nToken usage: input={usage.get('input_tokens', 'N/A')}, output={usage.get('output_tokens', 'N/A')}, total={usage.get('total_tokens', 'N/A')}")

        print("\n⏱️  Request sent. Data will appear in system.ai_gateway.usage after ingestion.")
        print("   Ingestion lag varies from minutes to hours — do not wait. Proceed to the next lab and revisit later.")
    else:
        print(f"\n⚠️  Request failed: {response.status_code}")
        print(f"Error: {response.text}")

except Exception as e:
    print(f"⚠️  Could not send request: {e}")
    print(f"\nTroubleshooting:")
    print(f"  - Ensure Model Service '{model_service_fqn}' exists in AI Gateway")
    print(f"  - Check that your workspace has access to the Model Service")
    print(f"  - Verify permissions on {catalog_name}.{schema_name} schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Query system.ai_gateway.usage (Important Setup Note)
# MAGIC
# MAGIC **After you send a real request**, you can query `system.ai_gateway.usage` to verify it was logged with your tags.
# MAGIC
# MAGIC ⚠️ **Ingestion Delay:** There is a variable delay between when your request completes and when it appears in `system.ai_gateway.usage`. This lag can range from **minutes to hours** depending on system table ingestion pipeline timing. **Do not wait** — proceed to the next lab and revisit this query later. The diagnostic cell below shows the current ingestion lag so you can understand the delay.

# COMMAND ----------

# First, let's see the schema of system.ai_gateway.usage in this workspace
# Run this to understand what fields are available

try:
    spark.sql("DESCRIBE TABLE system.ai_gateway.usage").display()
    print("✅ system.ai_gateway.usage is accessible in this workspace")
except Exception as e:
    print(f"⚠️  system.ai_gateway.usage may not be available: {e}")
    print("   This table requires Unity Catalog enablement and specific workspace configuration.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: How to Query Your Requests (After Ingestion)
# MAGIC
# MAGIC Once you've sent a real request and ingestion has caught up (check the diagnostic above), run a query like this:

# COMMAND ----------

# DBTITLE 1,Query system.ai_gateway.usage with ingestion lag diagnostic
print("Querying system.ai_gateway.usage for your tagged requests...\n")

try:
    # First, check overall ingestion lag (latest event vs. current time)
    lag_query = """
    SELECT 
      COUNT(*) as total_records,
      MAX(event_time) as latest_event,
      CURRENT_TIMESTAMP() as current_time,
      ROUND((unix_timestamp(CURRENT_TIMESTAMP()) - unix_timestamp(MAX(event_time))) / 60, 1) as ingestion_lag_minutes
    FROM system.ai_gateway.usage
    """
    lag_result = spark.sql(lag_query).collect()
    total_all = lag_result[0]['total_records']
    latest_all = lag_result[0]['latest_event']
    lag_min = lag_result[0]['ingestion_lag_minutes']
    
    print(f"📊 Ingestion Lag Diagnostic:")
    print(f"   Total records in table: {total_all:,}")
    print(f"   Latest event in table:  {latest_all}")
    print(f"   Current time:           {lag_result[0]['current_time']}")
    print(f"   Ingestion lag:          ~{lag_min} minutes" if lag_min is not None else "   Ingestion lag:          N/A (table is empty)")
    if lag_min and lag_min > 10:
        print(f"\n   ⚠️  The ingestion pipeline is currently {lag_min:.0f} minutes behind.")
        print(f"   Requests sent in the last {lag_min:.0f} minutes may not yet be visible.")
        print(f"   This is normal — proceed to the next lab and revisit this query later.")
    print()

    # Now check for recent data
    diagnostic_query = """
    SELECT COUNT(*) as total_records, MAX(event_time) as latest_event
    FROM system.ai_gateway.usage
    WHERE event_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
    """

    diagnostic_result = spark.sql(diagnostic_query).collect()
    total_records = diagnostic_result[0]['total_records']
    latest_event = diagnostic_result[0]['latest_event']

    print(f"Diagnostic: {total_records} records in last 2 hours, latest event: {latest_event}\n")

    if total_records == 0:
        print("⚠️  No data found in system.ai_gateway.usage for the last 2 hours.")
        print("   This could mean:")
        print("   - The table is still empty (no requests have been logged yet)")
        print("   - Try checking if the request actually completed successfully")
        print("   - Wait a bit longer and try again\n")

    # Now query for your specific endpoint (last 2 hours to catch recent requests)
    query = f"""
    SELECT
      request_id,
      event_time,
      endpoint_name,
      request_tags,
      input_tokens,
      output_tokens,
      latency_ms,
      status_code
    FROM system.ai_gateway.usage
    WHERE endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
    ORDER BY event_time DESC
    LIMIT 20
    """

    results_df = spark.sql(query)
    result_count = results_df.count()
    print(f"Found {result_count} requests for your endpoint in the last 2 hours:\n")
    if result_count > 0:
        results_df.display()
        print("\n✅ Requests found for your Model Service!")
        print(f"   ✓ endpoint_name shows your Model Service: {model_service_fqn}")
        print(f"   ✓ Check request_tags to verify the tags you sent in Step 4")
    else:
        print("\n⏱️  No requests found yet for your endpoint.")
        if lag_min is not None:
            print(f"   Current ingestion lag is ~{lag_min:.0f} minutes. Your request may not be visible yet.")
        else:
            print("   The usage table appears to be empty. No ingestion lag data available.")
        print("   Proceed to the next lab and revisit this query later.")

except Exception as e:
    print(f"⚠️  Query failed: {e}")
    print("   system.ai_gateway.usage may not be available in this workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Cost Attribution Flow
# MAGIC
# MAGIC Here's the complete flow:
# MAGIC
# MAGIC ```
# MAGIC You send a request with request tags
# MAGIC  ↓
# MAGIC Databricks-Ai-Gateway-Request-Tags header
# MAGIC  ↓
# MAGIC Unity Gateway routes through your Model Service
# MAGIC  ↓
# MAGIC Logs to system.ai_gateway.usage (after ingestion delay)
# MAGIC  ├─ request_tags field contains your tags
# MAGIC  ├─ endpoint_name shows your service FQN
# MAGIC  └─ request/response tokens show consumption
# MAGIC  ↓
# MAGIC For Databricks-hosted models:
# MAGIC  ├─ system.billing.usage records cost impact
# MAGIC  └─ Uses service tags (configured on the Model Service) for primary attribution
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Send a real request** to your Model Service (using your application, notebook, or test client)
# MAGIC   - Include the request tags in the HTTP header
# MAGIC   - Note: ingestion lag varies from minutes to hours
# MAGIC
# MAGIC 2. **Query system.ai_gateway.usage** using the query from Step 5
# MAGIC   - Verify your request appears with the tags you sent
# MAGIC   - Note the latency and token counts
# MAGIC
# MAGIC 3. **Proceed to Lab 1.2** to aggregate usage and understand cost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Tag Design**: What tags would *your* organization use? How would they differ from (team, project, cost_center, environment)?
# MAGIC 2. **Request vs. Service Tags**: When would you use request tags vs. configuring service tags? (Answer: request tags for call-by-call variation; service tags for service-wide defaults)
# MAGIC 3. **Visibility**: If you couldn't see `system.ai_gateway.usage`, how would you track who's calling your Model Service?
# MAGIC 4. **Cost Recovery**: If your team runs 10,000 requests with different project tags, how would you attribute cost back to each project?

# COMMAND ----------

# DBTITLE 1,Lab 1.1 Summary
print(f"""
✅ Lab 1.1 Overview Complete

Model Service ready: {model_service_fqn}

To use it:
1. Include request tags in the Databricks-Ai-Gateway-Request-Tags HTTP header
2. Send a request to {model_service_fqn}
3. Query system.ai_gateway.usage after ingestion delay
4. Verify your tags are recorded in the request_tags field

Next: Lab 1.2 — Query usage to understand cost attribution
""")