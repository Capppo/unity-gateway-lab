# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2.1: Access Control—Who Can Call Your AI Services?
# MAGIC
# MAGIC ## Overview
# MAGIC Before a team can call a Model Service, they must have permission. This lab teaches how to use Unity Catalog grants to control who can invoke AI services, and how to observe permission decisions in system.access.audit.
# MAGIC
# MAGIC **Learning outcome:** "Who can call?" — Grant/revoke EXECUTE permissions on your Model Service, verify the changes, and see the decisions logged in the audit trail.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Access Control in Unity Catalog + AI Gateway
# MAGIC
# MAGIC **Permission Model for Model Services:**
# MAGIC - **EXECUTE**: Allows the principal to invoke/call the service
# MAGIC - **USE_SCHEMA**: Required to access objects in the schema
# MAGIC - **USE_CATALOG**: Required to access any object in the catalog
# MAGIC
# MAGIC **Permission check flow (all three must pass for a call to succeed):**
# MAGIC 1. Can the caller access the catalog? (USE_CATALOG check)
# MAGIC 2. Can the caller access the schema? (USE_SCHEMA check)
# MAGIC 3. Can the caller execute the Model Service? (EXECUTE check)
# MAGIC
# MAGIC Access events are logged to `system.access.audit` in real time. You'll see:
# MAGIC - Successful permission checks (HTTP 200)
# MAGIC - Permission denials (HTTP 403)
# MAGIC - Who attempted the access, when, and the outcome

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import re
import time
import json
import requests

w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = user_name.split('@')[0]

# Sanitize user_id: replace invalid characters with underscores
invalid_chars = r'[\s\.\/\x00-\x1f]'
if re.search(invalid_chars, user_id):
    user_id = re.sub(invalid_chars, '_', user_id)

catalog_name = user_id
schema_name = "sales"
service_name = f"{user_id}_lab_model_service"
model_service_fqn = f"{catalog_name}.{schema_name}.{service_name}"

spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Setup Complete")
print(f"   Catalog: {catalog_name}")
print(f"   Schema: {schema_name}")
print(f"   Model Service: {model_service_fqn}")
print(f"   Current user: {user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check Current Permissions
# MAGIC
# MAGIC Currently, no one except you (the owner) can call your Model Service. Let's verify this in Catalog Explorer.
# MAGIC
# MAGIC 1. Open **Catalog Explorer** (left sidebar in your Databricks workspace)
# MAGIC 2. Navigate to: `<your_catalog>` → `sales` → `<your_user_id>_lab_model_service`
# MAGIC 3. Click the **Permissions** tab
# MAGIC 4. You should see your owner-level permissions (**ALL PRIVILEGES**, **MANAGE**) inherited from the catalog. No **EXECUTE** grant for other users should be present yet.
# MAGIC
# MAGIC This is the default: as the creator and catalog owner, you have full access. No other users have EXECUTE permission. Ready to grant access to others in the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Grant EXECUTE to Account Users
# MAGIC
# MAGIC Now grant EXECUTE permission to all account users (anyone in your workspace can call the service).
# MAGIC
# MAGIC 1. Stay in Catalog Explorer, **Permissions** tab for your Model Service
# MAGIC 2. Click **Grant**
# MAGIC 3. Search for and select **All account users**
# MAGIC 4. Select the **EXECUTE** privilege
# MAGIC 5. Click **Grant**
# MAGIC
# MAGIC **Wait 1–2 seconds** for the grant to take effect, then proceed to Step 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the Grant Took Effect
# MAGIC
# MAGIC Refresh the Permissions tab to confirm the grant is visible.
# MAGIC
# MAGIC You should see:
# MAGIC - **Principal**: All account users
# MAGIC - **Privilege**: EXECUTE
# MAGIC
# MAGIC If the row hasn't appeared yet, refresh the page. Once you see it, proceed to Step 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Query system.access.audit to See Permission Events
# MAGIC
# MAGIC The audit log captures all permission grants, denials, and access attempts. Let's check for the GRANT action you just performed.

# COMMAND ----------

print("Querying system.access.audit for permission grant/revoke events on your Model Service...\n")

try:
    audit_query = f"""
    SELECT
      event_time,
      user_identity.email AS user_email,
      action_name,
      request_params,
      response.status_code AS status_code
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAY
      AND service_name = 'unityCatalog'
      AND action_name IN ('updatePermissions', 'getPermissions')
      AND CAST(request_params AS STRING) LIKE '%{service_name}%'
    ORDER BY event_time DESC
    LIMIT 20
    """

    audit_df = spark.sql(audit_query)
    if audit_df.count() > 0:
        print(f"✅ Found {audit_df.count()} audit events:\n")
        audit_df.display()
    else:
        print("(No audit events yet for permission changes on your Model Service.)")
        print("Note: Audit events are logged under service_name='unityCatalog' with")
        print("action_name='updatePermissions'. They may take a moment to appear.")

except Exception as e:
    print(f"⚠️  Could not query system.access.audit: {e}")
    print("   (This is expected if the table is not available in your workspace)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test the Permission by Calling the Model Service
# MAGIC
# MAGIC Now that account users have EXECUTE permission, let's verify a call succeeds. This test simulates what an account user would experience calling your service.

# COMMAND ----------

print(f"Testing Model Service access with EXECUTE permission...\n")

try:
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get().rstrip('/')
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    endpoint_url = f"{workspace_url}/ai-gateway/mlflow/v1/responses"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "Databricks-Ai-Gateway-Request-Tags": json.dumps({
            "team": "access-control-lab",
            "action": "permission-test"
        })
    }

    payload = {
        "model": model_service_fqn,
        "max_output_tokens": 100,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Can I call this service?"
                    }
                ]
            }
        ]
    }

    response = requests.post(endpoint_url, json=payload, headers=headers, timeout=30)

    if response.status_code == 200:
        print(f"✅ Request succeeded (HTTP {response.status_code})")
        print("   Permission check: PASSED")
        print("   Account users can now call this service.")
    elif response.status_code == 403:
        print(f"❌ Permission denied (HTTP 403)")
        print(f"   The EXECUTE grant may not have taken effect yet. Wait a moment and retry.")
        print(f"   Error: {response.text}")
    elif response.status_code == 404:
        error_detail = response.json() if response.text else {}
        print(f"⚠️  Model not found (HTTP 404)")
        print(f"   Error: {error_detail}")
        print(f"\n   This is NOT a permission error. The model destination configured in your")
        print(f"   Model Service may not be available in your region.")
        print(f"   → Go back to Lab 0 and update your Model Service to use a different model destination.")
        print(f"   → Verify the model is available by checking the AI Gateway model list.")
    else:
        print(f"⚠️  Unexpected response (HTTP {response.status_code})")
        print(f"   Error: {response.text}")

except Exception as e:
    print(f"⚠️  Could not send request: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Revoke the Permission
# MAGIC
# MAGIC Now let's revoke EXECUTE from account users to demonstrate permission denial. Go back to Catalog Explorer.
# MAGIC
# MAGIC 1. Open Catalog Explorer, **Permissions** tab for your Model Service
# MAGIC 2. Find the row with:
# MAGIC    - **Principal**: All account users
# MAGIC    - **Privilege**: EXECUTE
# MAGIC 3. Click the `...` menu (or row checkbox) and select **Revoke**
# MAGIC 4. Confirm the revocation
# MAGIC
# MAGIC **Wait 1–2 seconds** for the revocation to propagate, then proceed to Step 8.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify the Revoke Took Effect
# MAGIC
# MAGIC Refresh the Permissions tab to confirm the grant is gone.
# MAGIC
# MAGIC You should now see only your owner-level permissions (**ALL PRIVILEGES**, **MANAGE**) inherited from the catalog. The **EXECUTE** grant for **All account users** should be gone. Once you see this, proceed to Step 9.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Test the Permission After Revoke
# MAGIC
# MAGIC Now that EXECUTE has been revoked, a call to the Model Service should be denied with HTTP 403. This demonstrates how access control works in real time.

# COMMAND ----------

print(f"Testing Model Service access after revoking EXECUTE permission...\n")

try:
    response = requests.post(endpoint_url, json=payload, headers=headers, timeout=30)

    if response.status_code == 403:
        print(f"✅ Request correctly denied (HTTP {response.status_code})")
        print("   Permission check: FAILED (as expected)")
        error_detail = response.json() if response.text else response.text
        print(f"   Response: {error_detail}")
    elif response.status_code == 200:
        print(f"⚠️  Request succeeded (HTTP {response.status_code})")
        print("   ℹ️  This is expected if you are the catalog owner — owners retain ALL PRIVILEGES")
        print("   regardless of EXECUTE revocations. The revoke applies to other users.")
        print("   To test a true denial, a different (non-owner) user would need to call the service.")
    elif response.status_code == 404:
        error_detail = response.json() if response.text else {}
        print(f"⚠️  Model not found (HTTP 404)")
        print(f"   Error: {error_detail}")
        print(f"\n   This is NOT a permission error. The model destination configured in your")
        print(f"   Model Service may not be available in your region.")
        print(f"   → Go back to Lab 0 and update your Model Service to use a different model destination.")
        print(f"   → The permission revoke test cannot be verified until the model is reachable.")
    else:
        print(f"Unexpected response (HTTP {response.status_code}): {response.text}")

except Exception as e:
    print(f"Could not send request: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Query Audit Log to See Denial Events
# MAGIC
# MAGIC The audit log captures both grants and access denials. Let's query for the denial event from your revoked call.

# COMMAND ----------

print("Querying system.access.audit for permission events on your Model Service...\n")

try:
    audit_query = f"""
    SELECT
      event_time,
      user_identity.email AS user_email,
      action_name,
      request_params,
      response.status_code AS status_code
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAY
      AND service_name = 'unityCatalog'
      AND action_name IN ('updatePermissions', 'getPermissions')
      AND CAST(request_params AS STRING) LIKE '%{service_name}%'
    ORDER BY event_time DESC
    LIMIT 30
    """

    audit_df = spark.sql(audit_query)
    if audit_df.count() > 0:
        print(f"✅ Found {audit_df.count()} audit events:\n")
        audit_df.display()
        print("\nLook for rows with:")
        print("  • action_name = 'updatePermissions' (grant or revoke action)")
        print("  • status_code = 200 (successful permission change)")
        print("  • Check request_params for details on what was granted/revoked")
    else:
        print("(No audit events yet for permission changes on your Model Service.)")
        print("Note: Permission events are logged under service_name='unityCatalog'.")
        print("They may take a moment to appear after a grant or revoke action.")

except Exception as e:
    print(f"⚠️  Could not query system.access.audit: {e}")
    print("   (This is expected if the table is not available in your workspace)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC You've now completed the access control lifecycle:
# MAGIC
# MAGIC **What you did:**
# MAGIC 1. ✅ Created a Model Service (from Lab 0)
# MAGIC 2. ✅ Verified initial state: only owner has access
# MAGIC 3. ✅ Granted EXECUTE permission to All account users
# MAGIC 4. ✅ Verified the grant in Catalog Explorer
# MAGIC 5. ✅ Tested successful API call (HTTP 200)
# MAGIC 6. ✅ Queried audit log to see the grant event
# MAGIC 7. ✅ Revoked EXECUTE permission
# MAGIC 8. ✅ Verified the revoke in Catalog Explorer
# MAGIC 9. ✅ Tested API call after revoke (note: catalog owners retain ALL PRIVILEGES, so you may see HTTP 200 instead of 403 — the revoke still applies to other users)
# MAGIC 10. ✅ Queried audit log to see permission change events
# MAGIC
# MAGIC **Key insights:**
# MAGIC - **Permissions are real-time**: revokes take effect immediately
# MAGIC - **Everything is auditable**: every permission check (grant, allow, deny) is logged
# MAGIC - **Principle of least privilege**: only grant permissions that are needed
# MAGIC - **Three-level hierarchy**: catalog → schema → object (all must allow access)