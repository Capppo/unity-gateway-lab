# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1.4: Traffic Splitting & Fallbacks—Optimize Routing and Resilience
# MAGIC
# MAGIC ## Overview
# MAGIC Beyond rate limits and budgets, you can control *which* backend handles each request. Traffic splitting lets you distribute load across multiple models (for A/B testing, gradual rollouts, or cost optimization). Fallbacks add resiliency by retrying failed requests against backup destinations. Both are observable in `system.ai_gateway.usage`.
# MAGIC
# MAGIC **Learning objective:** Configure traffic splitting to distribute requests intelligently and verify routing via observability queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Traffic Splitting vs. Fallbacks
# MAGIC
# MAGIC **Traffic Splitting:**
# MAGIC - Distributes incoming requests across multiple model backends by percentage
# MAGIC - Examples: 80% GPT-4o, 20% Claude (for A/B testing); 100% GPT-4 (gradual migration from GPT-3.5)
# MAGIC - Session affinity: Requests with the same session header stay on the same destination (prefix caching benefit)
# MAGIC - Enables: A/B testing, gradual rollouts, load balancing, cost optimization
# MAGIC
# MAGIC **Fallbacks:**
# MAGIC - Retries failed requests against backup destinations in priority order
# MAGIC - Works with or without traffic splitting
# MAGIC - When combined: traffic splitting picks primary destination, fallbacks retry if primary fails
# MAGIC - Increases availability and resilience
# MAGIC - Evidence: `system.ai_gateway.usage.routing_information` logs which destination handled the request

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
print(f"\nYou'll configure traffic splitting on this Model Service.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understanding Traffic Splitting
# MAGIC
# MAGIC **What it does:**
# MAGIC - Routes requests to multiple model backends according to configured percentages
# MAGIC - Example: Configure 70% of requests to GPT-4o, 30% to Claude
# MAGIC - Each request is routed randomly to one destination based on the percentages
# MAGIC - Over time, observed distribution converges to configured percentages
# MAGIC
# MAGIC **Session Affinity:**
# MAGIC - Requests that include a session-identifying header are pinned to the same destination
# MAGIC - Benefits: Prefix caching, consistent behavior within a session
# MAGIC - Requests without session headers follow the weighted split
# MAGIC
# MAGIC **Constraints:**
# MAGIC - Maximum 5 destinations per Model Service
# MAGIC - You cannot apply traffic splitting to fallback destinations (fallbacks are separate)

# COMMAND ----------

# DBTITLE 1,Traffic splitting example scenarios
scenarios = [
  {
    "scenario": "A/B Testing",
    "primary": "Claude (50%)",
    "secondary": "GPT-4o (50%)",
    "reason": "Compare model performance on user requests"
  },
  {
    "scenario": "Gradual Rollout",
    "primary": "GPT-3.5 (90%)",
    "secondary": "GPT-4 (10%)",
    "reason": "Slowly migrate to new model, monitor for issues"
  },
  {
    "scenario": "Cost Optimization",
    "primary": "Cheaper Model (70%)",
    "secondary": "Premium Model (30%)",
    "reason": "Use budget-friendly model for most requests, premium for high-stakes"
  },
  {
    "scenario": "Load Balancing",
    "primary": "Provider A (50%)",
    "secondary": "Provider B (50%)",
    "reason": "Spread load, avoid rate limits, improve latency"
  },
]

print("Common Traffic Splitting Use Cases:\n")
for s in scenarios:
  print(f"{s['scenario']}:")
  print(f"  {s['primary']} | {s['secondary']}")
  print(f"  Why: {s['reason']}")
  print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure Traffic Splitting in the UI
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. In Databricks, click **AI Gateway** → Select your Model Service
# MAGIC 2. Navigate to the **Routing** tab
# MAGIC 3. Click **Add another model** to add a second (or third, ..., fifth) destination
# MAGIC 4. For each destination, set **Traffic percentage**
# MAGIC    - Percentages must sum to 100%
# MAGIC 5. The system saves automatically when all allocations sum to 100%
# MAGIC
# MAGIC **Note:** This lab focuses on the configuration and observability. The actual setup happens in the UI.

# COMMAND ----------

print("""
✅ To configure traffic splitting on your Model Service:

1. Open AI Gateway in your workspace sidebar
2. Click on: {model_service_fqn}
3. Navigate to the **Routing** tab
4. Click "Add another model"
5. Select a second model backend (e.g., if your primary is GPT-4o, add Claude)
6. Set percentages:
   - Destination 1: 70%
   - Destination 2: 30%
7. Wait for system to save (should save automatically when percentages sum to 100%)

After configuration, wait 1-2 minutes for changes to propagate.
Then proceed to Step 4 to verify routing is working.
""".format(model_service_fqn=model_service_fqn))

# COMMAND ----------

# DBTITLE 1,Send test requests after configuring traffic split
# MAGIC %md
# MAGIC ## Step 3b: Send Test Requests Through Your Model Service
# MAGIC
# MAGIC **Important:** After configuring traffic splitting, you must send new requests through your Model Service so that routing data is generated. Queries in Step 4 look at `system.ai_gateway.usage`, which only contains data for requests that have actually been sent.
# MAGIC
# MAGIC Reuse the request code from **Lab 1.1 Step 4** to send several test requests. Send at least 5–10 requests so the traffic split distribution is observable.
# MAGIC
# MAGIC ⚠️ **Ingestion lag:** Even after sending requests, there is a delay (~minutes to ~45 minutes) before data appears in `system.ai_gateway.usage`. If Step 4 queries return empty results, revisit them later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Traffic Splitting with Observability
# MAGIC
# MAGIC After you configure traffic splitting and send requests through your Model Service, query `system.ai_gateway.usage` to verify that requests are being routed according to your configured percentages.

# COMMAND ----------

# DBTITLE 1,Query routing distribution
print("Querying system.ai_gateway.usage for routing distribution...\n")
print("This query shows:")
print("- destination: Which model backend handled the request")
print("- request_count: How many requests went to this destination")
print("- actual_pct: Observed percentage of traffic\n")

try:
    result_df = spark.sql(f"""
    SELECT
      destination_name AS destination,
      COUNT(*) AS request_count,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS actual_pct
    FROM system.ai_gateway.usage
    WHERE
      endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
    GROUP BY destination_name
    ORDER BY actual_pct DESC
    """)

    count = result_df.count()
    if count > 0:
        print(f"Found routing data for {count} destinations:\n")
        result_df.display()
    else:
        print("No routing data found yet. This is expected if you haven't sent requests with traffic splitting configured.")

except Exception as e:
    print(f"Note: Query failed: {e}")
    print("   system.ai_gateway.usage may not be available in this workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Understanding Fallbacks
# MAGIC
# MAGIC **What they do:**
# MAGIC - Retries failed requests against backup destinations in priority order
# MAGIC - Increases availability by providing redundancy
# MAGIC - Works with or without traffic splitting
# MAGIC
# MAGIC **When to use:**
# MAGIC - Primary model is unavailable (rate-limited or down)
# MAGIC - Gradual migration: keep old model as fallback during rollout
# MAGIC - Cost optimization: use cheap model primarily, fallback to premium if it fails
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Traffic splitting selects a primary destination
# MAGIC 2. Request sent to primary
# MAGIC 3. If primary fails (429, 5xx, timeout), system retries fallback destinations in order
# MAGIC 4. First fallback to succeed becomes the response
# MAGIC 5. If all fallbacks fail, request returns error to user

# COMMAND ----------

# DBTITLE 1,Fallback configuration example
print("""
Fallback Configuration Example:

Primary (from traffic split): GPT-4o (70%)
Fallback 1: Claude (added first, tried first if primary fails)
Fallback 2: GPT-3.5 (added second, tried if first fallback fails)

Request flow:
1. Traffic split selects GPT-4o → send request
2. If GPT-4o fails (429, 5xx, timeout): retry Claude (first fallback)
3. If Claude fails: retry GPT-3.5 (second fallback)
4. If GPT-3.5 succeeds: return response
5. If all fail: request returns error to user

Key: The order you add fallbacks determines the cascade order.
The system tries them in the order they appear (first added = tried first).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Configure Fallbacks in the UI
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. In your Model Service settings, navigate to the fallback configuration section
# MAGIC 2. Add a backup model (must be different from primary/traffic split destinations)
# MAGIC 3. Configure additional fallbacks if desired
# MAGIC 4. The fallback priority order determines which destination is tried first after a primary failure
# MAGIC 5. Save the configuration
# MAGIC
# MAGIC **Note:** Fallbacks are configured alongside traffic splitting and can be used independently or together. The configured fallback order determines the cascade priority.

# COMMAND ----------

print("""
✅ To configure fallbacks on your Model Service:

1. Open AI Gateway → Your Model Service
2. Navigate to the fallback configuration section
3. Add a backup model
4. Add additional fallbacks if desired
5. Configure the fallback priority order (determines which is tried first after primary failure)
6. Save

After configuration, wait 1-2 minutes for changes to propagate.

To verify fallbacks are working:
- Query system.ai_gateway.usage with a filter for failed requests
- Look for routing_information field to see which fallback was used
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Evidence & Observability
# MAGIC
# MAGIC Both traffic splitting and fallbacks are logged in `system.ai_gateway.usage`.

# COMMAND ----------

# DBTITLE 1,Query fallback usage
print("Querying system.ai_gateway.usage for fallback routing details...\n")

try:
    result_df = spark.sql(f"""
    SELECT
      event_time,
      request_id,
      destination_name AS final_destination,
      status_code AS final_status_code,
      routing_information
    FROM system.ai_gateway.usage
    WHERE
      endpoint_name = '{model_service_fqn}'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
      AND routing_information IS NOT NULL
    ORDER BY event_time DESC
    LIMIT 20
    """)

    count = result_df.count()
    if count > 0:
        print(f"Found {count} requests with routing information:\n")
        result_df.display()
        print("\nLook for:")
        print("  - final_destination: Which model actually handled the request (after any fallbacks)")
        print("  - routing_information: STRUCT with attempts array (priority, action, destination, status_code)")
    else:
        print("No requests with routing information found in the last 7 days.")
        print("This is expected if you haven't configured traffic splitting or fallbacks yet.")

except Exception as e:
    print(f"Note: Query failed: {e}")
    print("   routing_information field may not be available in this workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Limitations & Best Practices
# MAGIC
# MAGIC **Limitations:**
# MAGIC - Traffic splitting supports a maximum of 5 destinations
# MAGIC - Cannot apply traffic splitting percentages to fallback destinations (they are independent)
# MAGIC - Traffic split percentages must sum to exactly 100%
# MAGIC - Traffic splitting is not reapplied during fallback retries (fallbacks are tried sequentially after primary failure)
# MAGIC
# MAGIC **Best Practices:**
# MAGIC 1. **Start small:** Configure traffic split with 2 destinations, verify before expanding
# MAGIC 2. **Monitor latency:** Different models may have different response times
# MAGIC 3. **A/B test carefully:** Ensure both models produce similar quality for fair comparison
# MAGIC 4. **Use fallbacks for resilience, not cost:** Fallbacks should be truly backup models, not primary optimizations
# MAGIC 5. **Query observability regularly:** Verify actual routing matches your config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **A/B Testing**: You want to compare GPT-4o vs. Claude on user requests. How would you configure traffic splitting? What metrics would you monitor?
# MAGIC
# MAGIC 2. **Gradual Rollout**: You're migrating from GPT-3.5 to GPT-4. You want to start with 5% of traffic on GPT-4 and gradually increase. How would you stage this in weekly increments?
# MAGIC
# MAGIC 3. **Cost Optimization**: You have a budget model (cheap) and a premium model (expensive, high quality). How would you configure traffic and fallbacks to optimize cost while maintaining quality?
# MAGIC
# MAGIC 4. **Session Affinity**: Why is it important for a user's conversation to stay on the same model during a session, even with traffic splitting?
# MAGIC
# MAGIC 5. **Fallback Strategy**: If your primary model is rate-limited, should you fall back to an equally expensive model or a cheaper one? What are the tradeoffs?

# COMMAND ----------

# DBTITLE 1,Lab 1.4 Summary
print(f"""
✅ Lab 1.4 Complete

What you learned:
1. Traffic splitting distributes requests across multiple backends by percentage
2. Session affinity keeps requests from the same session on the same destination (prefix caching)
3. Fallbacks add resilience by retrying failed requests against backup destinations in priority order
4. Both are observable in system.ai_gateway.usage routing_information

Configuration summary:
- Traffic split: AI Gateway → Model Service → Routing tab → Add another model, set percentages (must sum to 100%)
- Fallbacks: AI Gateway → Model Service → configure fallback priority order
- Verification: Query system.ai_gateway.usage for destination_name, status_code, and routing_information

Next: Lab 2.1 — Shift to access governance (permissions & audit).
""")