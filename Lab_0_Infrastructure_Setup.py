# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 0: Infrastructure Setup for AI Governance
# MAGIC
# MAGIC ## Overview
# MAGIC This lab establishes your governed namespace: your pre-provisioned user-scoped catalog and a schema in Unity Catalog where you'll create a Model Service. **You will not create observability tables in this lab.** Governance infrastructure is provided by:
# MAGIC - **Unity Catalog**: Namespace and permission model (USE_CATALOG, USE_SCHEMA, EXECUTE on services) — your catalog is pre-created
# MAGIC - **Unity Gateway**: Traffic routing, usage logging, policies, and rate limits (Beta features require explicit enablement)
# MAGIC - **System tables**: `system.ai_gateway.usage` (requests), `system.billing.usage` (cost), `system.access.audit` (Public Preview access events)
# MAGIC
# MAGIC **Learning outcomes:**
# MAGIC - Reference your user-scoped catalog in Unity Catalog
# MAGIC - Create a schema within your catalog for organized resource management
# MAGIC - Understand that a Model Service is a fully qualified Unity Catalog object (`<catalog>.<schema>.<service>`)
# MAGIC - Create a Model Service as the target for governance labs
# MAGIC - Know where evidence of requests, costs, and access decisions live

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and User Context
# MAGIC
# MAGIC We'll use your user ID to create a unique namespace for all labs, ensuring clean isolation.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = user_name.split('@')[0]

# Sanitize user_id: replace invalid characters (spaces, periods, slashes, control chars) with underscores
import re
invalid_chars = r'[\s\.\/\x00-\x1f]'
if re.search(invalid_chars, user_id):
    user_id = re.sub(invalid_chars, '_', user_id)

print(f"Current User: {user_name}")
print(f"User ID for naming: {user_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Reference Your User Catalog and Create Schema
# MAGIC
# MAGIC Your catalog has been provisioned for you by workshop admins. It is scoped to your user ID. All governance work in this workshop happens in your catalog, in a `sales` schema. This ensures clean data boundaries and realistic security posture.

# COMMAND ----------

# DBTITLE 1,Reference your user catalog (pre-provisioned)
catalog_name = user_id
spark.sql(f"USE CATALOG `{catalog_name}`")

print(f"✅ Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create sales schema
spark.sql("CREATE SCHEMA IF NOT EXISTS sales COMMENT 'Schema for AI governance lab exercises'")
spark.sql("USE SCHEMA sales")

print(f"✅ Schema created: {catalog_name}.sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: (Optional) Create a Mock Observability Table
# MAGIC
# MAGIC **Note:** This table is mock application-owned telemetry, not a Unity Gateway system table. It's optional and only for learners who want to practice logging. All real governance evidence comes from `system.ai_gateway.usage`, `system.billing.usage`, and `system.access.audit`.

# COMMAND ----------

# DBTITLE 1,Create mock governance_events table (optional)
# This is a learner-created table for practice, not a product-managed system table.
spark.sql("""
CREATE TABLE IF NOT EXISTS governance_events (
  id STRING COMMENT 'Request ID (example mock)',
  event_time TIMESTAMP COMMENT 'When the request happened (example mock)',
  team STRING COMMENT 'Team tag (example mock)'
)
USING DELTA
COMMENT 'Mock application-owned observability table (not managed by Unity Gateway)'
""")

print("✅ Mock table created: governance_events (for learning purposes only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Observability Tables
# MAGIC
# MAGIC Confirm the tables exist and are empty, ready for lab exercises.

# COMMAND ----------

# DBTITLE 1,List tables in sales schema
spark.sql("SHOW TABLES IN sales").display()

# COMMAND ----------

# DBTITLE 1,Verify table schema
spark.sql("DESCRIBE TABLE EXTENDED governance_events").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create a Model Service in Unity Catalog
# MAGIC
# MAGIC A **Model Service** is a Unity Catalog object (like a table or schema) that references an AI model destination. It is not a deployment or compute resource—it is a governance object. When you call a Model Service by its fully qualified name, your request is routed through Unity Gateway, which can:
# MAGIC - Track usage in `system.ai_gateway.usage`
# MAGIC - Enforce rate limits (QPM, TPM)
# MAGIC - Apply service policies (Beta, optional)
# MAGIC - Log access attempts to `system.access.audit`
# MAGIC
# MAGIC **Steps to create a Model Service:**
# MAGIC 1. Open **Catalog Explorer** in the left sidebar
# MAGIC 2. Navigate to your catalog → `sales` schema
# MAGIC 3. Click **Create** → **Service** → **Model service**
# MAGIC 4. Name it: `<your_user_id>_lab_model_service` (e.g., `sean_fiene_lab_model_service`)
# MAGIC 5. Select a destination from the dropdown (available models vary by region). **Check the AI Gateway model list in your workspace for currently available models.** Verify the chosen model is available by sending a test request after creation
# MAGIC 6. Click **Create**
# MAGIC
# MAGIC Your Model Service is now registered as: **`<your_catalog>.sales.<your_user_id>_lab_model_service`**
# MAGIC
# MAGIC Use this fully qualified name (FQN) to invoke the service in Labs 1–2. You will need EXECUTE privilege on the service to call it.

# COMMAND ----------

# DBTITLE 1,Validate Model Service destination is reachable
# Validate that your Model Service destination responds correctly
import requests as req
import json

# Construct the Model Service FQN from variables defined above
service_name = f"{user_id}_lab_model_service"
model_service_fqn = f"{catalog_name}.sales.{service_name}"

try:
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get().rstrip('/')
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    endpoint_url = f"{workspace_url}/ai-gateway/mlflow/v1/responses"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "model": model_service_fqn,
        "max_output_tokens": 50,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "Say hello in one word."}]}]
    }

    print(f"Testing Model Service: {model_service_fqn}...")
    response = req.post(endpoint_url, json=payload, headers=headers, timeout=30)

    if response.status_code == 200:
        print(f"\n✅ Model Service is reachable (HTTP 200). You're ready for Labs 1-2!")
    elif response.status_code == 404:
        error_detail = response.json() if response.text else {}
        print(f"\n❌ Model Service not found (HTTP 404): {error_detail}")
        print(f"   Have you completed Step 5 (creating the Model Service in Catalog Explorer)?")
        print(f"   → If not, go to Step 5 above and create your Model Service first.")
        print(f"   → If you already created it, the model destination may not be available in your region.")
        print(f"   → Check the AI Gateway model list for available models and update your Model Service.")
    else:
        print(f"\n⚠️  Unexpected response (HTTP {response.status_code}): {response.text}")

except Exception as e:
    print(f"⚠️  Could not test Model Service: {e}")
    print(f"   Ensure the Model Service '{model_service_fqn}' exists before proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC **Infrastructure provisioned:**
# MAGIC - ✅ Catalog: `<your_catalog>` (Unity Catalog namespace)
# MAGIC - ✅ Schema: `<your_catalog>.sales` (sub-namespace)
# MAGIC - ✅ Model Service: `<your_catalog>.sales.<your_user_id>_lab_model_service` (governance object)
# MAGIC - ✅ Namespace permissions: You have ownership; callers need EXECUTE
# MAGIC
# MAGIC **Evidence sources for Labs 1–2:**
# MAGIC - `system.ai_gateway.usage`: Tracks requests, tokens, and request tags per call
# MAGIC - `system.billing.usage`: Databricks-hosted model cost attribution
# MAGIC - `system.access.audit` (Public Preview): Logs access attempts and permission outcomes
# MAGIC
# MAGIC **Next: Lab 1.1** — Send a tagged request to your Model Service and verify it appears in system tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **FQN and Access**: Your Model Service is `<your_catalog>.sales.<your_user_id>_lab_model_service`. Who else in your organization would be able to call it, and why?
# MAGIC 2. **Isolation vs. Sharing**: What's the difference between a user-scoped catalog (like this lab) and a shared team catalog in production? When would you use each?
# MAGIC 3. **Evidence Sources**: When you call your Model Service in Lab 1.1, where will evidence appear?
# MAGIC    - `system.ai_gateway.usage`? (request-level telemetry)
# MAGIC    - `system.billing.usage`? (cost attribution, Databricks-hosted only)
# MAGIC    - `system.access.audit`? (permission check outcomes, if applicable)
# MAGIC    - All three? (answer: depends on what you're looking for)
# MAGIC 4. **Permissions and Privileges**: What privilege do you need to call this Model Service? What privilege would you grant to a colleague?

# COMMAND ----------

# DBTITLE 1,Lab 0 Setup Complete
print(f"""
✅ Lab 0 Setup Complete

Your governed namespace:
- Catalog: {catalog_name}
- Schema: {catalog_name}.sales
- Model Service: {catalog_name}.sales.{user_id}_lab_model_service (create via Catalog Explorer, Step 5)

Evidence sources for this workshop:
- system.ai_gateway.usage: Request-level usage and tags
- system.billing.usage: Cost attribution (Databricks-hosted models)
- system.access.audit: Access attempts and decisions (Public Preview)

Next steps:
1. Create your Model Service in Unity Catalog (Step 5 above)
2. Proceed to Lab 1.1 to send your first tagged request
""")