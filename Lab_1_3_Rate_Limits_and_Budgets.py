# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1.3: Rate Limits and Budgets — Two Different Controls
# MAGIC
# MAGIC ## Overview
# MAGIC You can now see what's happening (Labs 1.1–1.2). This lab introduces two *different* ways to prevent runaway costs:
# MAGIC
# MAGIC 1. **Rate Limits** (throughput controls): "This Model Service can handle max 100 requests/minute" or "max 10M tokens/day"
# MAGIC 2. **Budgets** (spend controls): "Team X can spend max $5k/month on AI"
# MAGIC
# MAGIC They sound similar, but they serve different purposes and are enforced differently.
# MAGIC
# MAGIC **Learning outcome:** Understand the difference between throughput limits and spend budgets, and know which evidence source shows whether they're working.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Rate Limits vs. Budgets
# MAGIC
# MAGIC **Rate Limits** (Throughput Controls):
# MAGIC - Limits on how many requests or tokens can flow through a service
# MAGIC - Measured in: Requests per minute/hour, Tokens per minute/hour
# MAGIC - Scopes: Endpoint-wide, per-user, per-group, per-service-principal
# MAGIC - Enforcement: Active (immediate HTTP 429 when exceeded)
# MAGIC - Purpose: Protect infrastructure from overload, fair resource sharing
# MAGIC - Evidence: `system.ai_gateway.usage` shows request count and tokens
# MAGIC
# MAGIC **Budgets** (Spend Controls):
# MAGIC - Limits on how much *money* can be spent
# MAGIC - Measured in: USD per month
# MAGIC - Enforcement: Near-real-time (approximate), not absolute
# MAGIC - Scopes: Shared team budget, per-user override, cost-center allocation
# MAGIC - Purpose: Prevent billing surprises, align incentives
# MAGIC - Evidence: `system.billing.usage` shows actual charges
# MAGIC
# MAGIC **Key difference**: A rate limit prevents you from making 1 million requests in an hour (good for infrastructure). A budget prevents you from spending $100k in an hour (good for your wallet)."

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

print(f"Model Service FQN: {model_service_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understanding Rate Limits
# MAGIC
# MAGIC **Rate limits are configured on the Model Service itself.**
# MAGIC
# MAGIC You would normally set them in the Unity Gateway UI:
# MAGIC 1. Open your Model Service in AI Gateway
# MAGIC 2. Navigate to the **Rate limits** section (may appear as a tab or in the service configuration panel)
# MAGIC 3. Configure limits in two scopes:
# MAGIC    - **Endpoint**: Set rate limits for the entire Model Service (all users combined)
# MAGIC    - **Per user**: Set limits per individual user, group, or service principal
# MAGIC 4. For each area, you can configure:
# MAGIC    - **Requests limit** (optional)
# MAGIC    - **Tokens limit** (optional)
# MAGIC    - **Duration**: minute or hour
# MAGIC 5. In Per user, "Default" applies to all users unless overridden. Click **Add user** to configure custom limits for specific users/groups/service principals
# MAGIC
# MAGIC When a request exceeds the limit, the caller receives:
# MAGIC ```
# MAGIC HTTP 429 Too Many Requests
# MAGIC ```
# MAGIC
# MAGIC Rate limits are *active immediately*. They don't require policies (Lab 2.3 covers policies).

# COMMAND ----------

# DBTITLE 1,How to configure rate limits
print("""
✅ To set up rate limits on your Model Service:

1. Open your Model Service in AI Gateway
2. Navigate to the Rate limits section
3. Configure limits in the available scopes:

   Endpoint (entire service):
   - Set Requests per minute/hour (optional)
   - Set Tokens per minute/hour (optional)

   Per user (individual control):
   - Default: applies to all users (configure requests/tokens + duration)
   - Click "Add user" to add specific users/groups/service principals
   - Each can have custom requests/tokens + duration

4. Save

After configuration, rate limits are enforced immediately.
""")

# COMMAND ----------

# DBTITLE 1,Example: What happens when a rate limit is breached
print("""
Example scenario: You set a rate limit of 100 requests per minute on the Endpoint.

Timeline:
- 10:00:00 AM: User A sends request #1 ✅ (1 total this minute, within limit)
- 10:00:01 AM: User B sends request #2 ✅ (2 total, still within limit)
- ...
- 10:00:45 AM: User Z sends request #101 ❌ (Rate limit exceeded for this minute)
  → Returns: HTTP 429 Too Many Requests
  → User must retry next minute or request higher limit

The actual rate limit is enforced by Unity Gateway.
Here, we're focused on understanding what it is and when it applies.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Understanding Budgets
# MAGIC
# MAGIC **Budgets are configured at the account level, not on the service.**
# MAGIC
# MAGIC You would normally set them in Account Console → Usage → Budgets:
# MAGIC 1. Click **Add budget** / **Create budget**
# MAGIC 2. Select **Unity AI Gateway** as the resource type
# MAGIC 3. Define a monthly budget (e.g., "$5,000/month for team X")
# MAGIC 4. Set alert thresholds (e.g., "alert at 80% of budget")
# MAGIC 5. Choose action on breach: "alert only" or "block new requests"
# MAGIC 6. Configure shared thresholds and/or per-user overrides
# MAGIC
# MAGIC When a team exceeds their monthly budget:
# MAGIC - If "alert only": Team keeps incurring charges; you get notified
# MAGIC - If "block": New requests are rejected; team can't spend more this month
# MAGIC
# MAGIC **Important**: Budget enforcement is *approximate near-real-time*, not absolute.
# MAGIC - A request that would exceed the budget might still go through (system lag)
# MAGIC - Actual charges from providers may differ from real-time estimates
# MAGIC - Never treat a budget as a hard cap—always check `system.billing.usage` for actual cost

# COMMAND ----------

# DBTITLE 1,Example: What happens when a budget is exceeded
print("""
Example scenario: Team "data-science" has a $5,000/month budget.

Timeline:
- Month-end: Team has spent $4,900 so far
- User sends a request that would cost $150
- System checks: $4,900 + $150 = $5,050 (exceeds budget)
- Decision depends on configuration:

  IF "alert only":
    → Request proceeds ✅ (charges $150)
    → Alert sent to finance
    → Actual total: $5,050 (over budget)

  IF "block new requests" is enabled:
    → Request may be rejected ❌ (or rejected after request is initiated)
    → User notified: "Budget exceeded"
    → Actual total: may exceed $4,900 due to enforcement lag

Important: Budget enforcement is **approximate and near-real-time**, not absolute.
- Active requests are not retroactively cancelled if budget is exceeded
- Spend may overshoot the threshold before enforcement takes effect
- Budget data comes from system.billing.usage with hourly/daily refresh lag
- Blocking is a best-effort protection, not a hard guarantee
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Monitoring Rate Limits and Budgets
# MAGIC
# MAGIC **To verify rate limits are working:**
# MAGIC
# MAGIC Query `system.ai_gateway.usage` and look for HTTP 429 errors or request rejections

# COMMAND ----------

# DBTITLE 1,Query for rate-limit rejections (429 responses)
print("Querying system.ai_gateway.usage for rate-limit rejections (HTTP 429)...\n")

try:
    result_df = spark.sql(f"""
    SELECT
      request_id,
      event_time,
      endpoint_name,
      requester,
      request_tags,
      status_code
    FROM system.ai_gateway.usage
    WHERE endpoint_name = '{model_service_fqn}'
      AND status_code = 429
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
    ORDER BY event_time DESC
    LIMIT 20
    """)

    count = result_df.count()
    if count > 0:
        print(f"Found {count} rate-limited requests (HTTP 429):\n")
        result_df.display()
    else:
        print("No rate-limited requests (HTTP 429) found in the last 24 hours.")
        print("This is expected if you haven't configured rate limits yet or haven't exceeded them.")

except Exception as e:
    print(f"Note: Query failed: {e}")
    print("   system.ai_gateway.usage may not be available in this workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Monitoring Budgets
# MAGIC
# MAGIC **To verify budgets are working:**
# MAGIC
# MAGIC Query `system.billing.usage` to see current spend vs. budget

# COMMAND ----------

# DBTITLE 1,Query team spend to compare against budget
print("Querying system.billing.usage for Model Serving spend...\n")

try:
    result_df = spark.sql("""
    SELECT
      custom_tags['team'] AS team,
      YEAR(usage_date) AS year,
      MONTH(usage_date) AS month,
      SUM(usage_quantity) AS total_dbu,
      CASE
        WHEN SUM(usage_quantity) > 5000 THEN '❌ OVER'
        WHEN SUM(usage_quantity) > 4000 THEN '⚠️ WARNING (80%)'
        ELSE '✅ OK'
      END AS budget_status
    FROM system.billing.usage
    WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE())
      AND billing_origin_product = 'MODEL_SERVING'
      AND usage_unit = 'DBU'
    GROUP BY 1, 2, 3
    ORDER BY total_dbu DESC
    """)

    count = result_df.count()
    if count > 0:
        print(f"Found {count} billing entries:\n")
        result_df.display()
        print("\nNotes:")
        print("- Budget thresholds (5000, 4000) are example DBU amounts—adjust for your org")
    else:
        print("No billing data found for MODEL_SERVING this month.")
        print("Billing data typically refreshes within 12 hours. Check back after the workshop.")

except Exception as e:
    error_msg = str(e)
    if 'PERMISSION_DENIED' in error_msg or 'permission' in error_msg.lower():
        print(f"⚠️  Permission denied — your account may not have access to system.billing.usage.")
        print("   Contact your workspace admin to grant USE SCHEMA on system.billing.")
    else:
        print(f"Note: Query failed: {e}")
        print("   system.billing.usage may not be available yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Differences Recap
# MAGIC
# MAGIC | Aspect | Rate Limits | Budgets |
# MAGIC |--------|-------------|---------|
# MAGIC | What it controls | Throughput (requests/tokens per minute or hour) | Spend (dollars per month) |
# MAGIC | Where to set | Model Service → Governance setup → Rate limits | Account Console → Usage → Budgets |
# MAGIC | Scopes | Endpoint and Per user (default + custom per user/group/service principal) | Team or cost center |
# MAGIC | When enforced | Immediately (active) | Near-real-time (approximate) |
# MAGIC | On breach | HTTP 429 (immediate) | Alert or block (configurable) |
# MAGIC | Evidence source | `system.ai_gateway.usage` | `system.billing.usage` |
# MAGIC | Absolute? | Yes (hard limit) | No (may overshoot slightly) |
# MAGIC
# MAGIC **In production, use both.** Rate limits protect infrastructure. Budgets protect your cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Rate Limit Strategy**: If data-science team makes 1000 requests/day with 10 tokens each, what requests-per-minute limit would you set? (Hint: 1000 requests/day ≈ 0.7 requests/min on average, but peak could be higher. Would you set Endpoint or Per user limits?)
# MAGIC
# MAGIC 2. **Per-User Overrides**: You have a 100 requests/minute Endpoint limit with 50 requests/minute Default (per user). When would you use "Add user" to give a specific user a custom higher limit?
# MAGIC
# MAGIC 3. **Budget Allocation**: How would you split a $100k/month AI budget across teams? What factors would you consider?
# MAGIC
# MAGIC 4. **Budget vs. Rate Limit**: A team hits their monthly budget halfway through the month. They ask for a limit increase. Do you increase the rate limit or the budget? (Answer: increase the budget if additional spending is approved. Increase the rate limit only when the problem is insufficient request/token throughput. Rate limits can reduce spend indirectly, but they are not dollar-based controls.)
# MAGIC
# MAGIC 5. **Monitoring Gap**: If a request costs $50 and you have a rate limit of 100 requests/minute, but a $1k/month budget, what could go wrong? (Answer: A rate limit of 100 requests/min could accumulate to 100 × $50 = $5k/min of theoretical spend. The rate limit alone doesn't enforce spending limits—it caps throughput. A $1k/month budget *can* block further requests once reached, but enforcement is approximate and may allow requests to go slightly over the threshold before blocking kicks in.)

# COMMAND ----------

# DBTITLE 1,Lab 1.3 Summary
print(f"""
✅ Lab 1.3 Overview Complete

What you've learned:
1. Rate limits control *throughput* (requests/tokens per minute or hour)
2. Budgets control *spend* (dollars per month)
3. They're enforced differently, refreshed differently, and monitored differently
4. You need both for complete cost governance

Configuration:
- Rate limits: Model Service → Rate limits section → Configure Endpoint and Per user limits
- Budgets: Account Console → Usage → Budgets → Add budget, select Unity AI Gateway as resource type

Next: Lab 1.4 — Traffic Splitting and Fallbacks
""")