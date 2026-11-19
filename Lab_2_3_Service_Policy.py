# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2.3: Service Policies - Request and Response Filtering
# MAGIC
# MAGIC ## Overview
# MAGIC Service policies let you inspect and govern requests *before* they reach the model (ON_CALL) and responses *after* the model generates them (ON_RESULT). This lab consolidates both approaches into a single workflow.
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Create two custom policy functions in your schema
# MAGIC 2. Attach a built-in guardrail (Unsafe Content Detection)
# MAGIC 3. Attach your custom ON_CALL policy (request filtering)
# MAGIC 4. Attach your custom ON_RESULT policy (response filtering)
# MAGIC 5. Test all three policies together in the Chat playground
# MAGIC
# MAGIC **Learning outcome:** Build a layered, defense-in-depth governance chain that protects both requests and responses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context: Policy Evaluation Points
# MAGIC
# MAGIC ```
# MAGIC Request → [ON_CALL: block bad requests] → Model → [ON_RESULT: block bad responses] → Response
# MAGIC ```
# MAGIC
# MAGIC - **ON_CALL** runs BEFORE the model (saves tokens by blocking bad input early)
# MAGIC - **ON_RESULT** runs AFTER the model (protects users from bad output)
# MAGIC - **Built-in guardrails** are maintained by Databricks; **custom policies** are SQL UDFs you write
# MAGIC - All policies are composable: run multiple policies in rank order, stop at first DENY

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json
import re
import requests
import time

w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = user_name.split('@')[0]

# Sanitize user_id: replace invalid characters (spaces, periods, slashes, control chars) with underscores
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
print(f"   User: {user_name}")
print(f"   Catalog: {catalog_name}")
print(f"   Schema: {schema_name}")
print(f"   Model Service: {model_service_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Custom Policy Functions
# MAGIC
# MAGIC Custom policies are SQL UDFs that examine requests or responses and return ALLOW, DENY, or ASK.
# MAGIC
# MAGIC We'll create two functions in your schema:
# MAGIC 1. **block_confidential_codename**: Blocks requests containing "project aurora"
# MAGIC 2. **block_unsafe_links**: Blocks responses containing URLs (`http://`, `https://`) or `javascript:` protocol links

# COMMAND ----------

# Function 1: ON_CALL policy - block confidential codename in requests
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.block_confidential_codename(
  event VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN event:type::string = 'request'
      AND contains(lower(event:context.message::string), 'project aurora')
    THEN to_variant_object(named_struct('result', 'DENY', 'reason', 'Requests about confidential projects are not permitted.'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
  END
""")

# COMMAND ----------

# Function 2: ON_RESULT policy - block unsafe links in responses
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.block_unsafe_links(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN event:type::string = 'response'
      AND (contains(lower(CAST(event:context AS STRING)), 'http://')
        OR contains(lower(CAST(event:context AS STRING)), 'https://')
        OR contains(lower(CAST(event:context AS STRING)), 'javascript:'))
    THEN to_variant_object(named_struct('result', 'DENY', 'reason', 'Response contained an insecure link and was blocked by policy.'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
  END;
""")

# COMMAND ----------

print(f"""
✅ Custom Functions Created:
   1. {catalog_name}.{schema_name}.block_confidential_codename(event VARIANT) → DENY if "project aurora" in request
   2. {catalog_name}.{schema_name}.block_unsafe_links(event VARIANT) → DENY if any URL (http://, https://) or "javascript:" in response

These functions will be attached to your Model Service in the next steps via the AI Gateway UI.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Understand Your Policy Stack
# MAGIC
# MAGIC You now have **three policies** that will work together:
# MAGIC
# MAGIC 1. **Built-in Unsafe Content** (Input guardrails)
# MAGIC    - Detects harmful or unsafe content in requests
# MAGIC    - Maintained by Databricks
# MAGIC    - Runs first before custom policies
# MAGIC
# MAGIC 2. **Custom block_confidential_codename** (Input guardrails)
# MAGIC    - Your business rule: block mentions of "project aurora"
# MAGIC    - Runs after built-in guardrail
# MAGIC    - Rank: 2
# MAGIC
# MAGIC 3. **Custom block_unsafe_links** (Output guardrails)
# MAGIC    - Your business rule: block responses with unsafe link patterns
# MAGIC    - Runs after the model generates a response
# MAGIC    - Only output guardrail, so runs first in that phase
# MAGIC
# MAGIC **Policy Chain Evaluation:**
# MAGIC ```
# MAGIC Request arrives
# MAGIC  ↓
# MAGIC Input Guardrails (Before the Model):
# MAGIC  • Run Built-in Unsafe Content
# MAGIC  • Run Custom block_confidential_codename
# MAGIC  ↓ (if both ALLOW)
# MAGIC Model processes request
# MAGIC  ↓
# MAGIC Model returns response
# MAGIC  ↓
# MAGIC Output Guardrails (After the Model):
# MAGIC  • Run Custom block_unsafe_links
# MAGIC  ↓ (if ALLOW)
# MAGIC Response sent to user
# MAGIC ```

# COMMAND ----------

print(f"""
✅ Policy Stack Configuration:

Input guardrails (Before the Model):
  1. Built-in: Unsafe Content
  2. Custom: {catalog_name}.{schema_name}.block_confidential_codename

Output guardrails (After the Model):
  1. Custom: {catalog_name}.{schema_name}.block_unsafe_links [Rank 1]

Model Service: {model_service_fqn}

You will attach each policy via the AI Gateway UI in the next steps.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Attach Policies via AI Gateway UI
# MAGIC
# MAGIC Now you'll attach the three policies to your Model Service. Follow these steps carefully.
# MAGIC
# MAGIC ### Step 3a: Attach Built-in Unsafe Content Guardrail
# MAGIC
# MAGIC 1. Open **Databricks Workspace** → Click the **AI Gateway** icon in the sidebar (left panel)
# MAGIC 2. Find and click your Model Service (printed in Step 1 output above)
# MAGIC 3. Click the **Policies** tab
# MAGIC 4. Click **New Policy**
# MAGIC 5. Under **Guardrail Type**, select **Built-in** → **Unsafe Content**
# MAGIC 6. Under **Guardrail scope**, check **Input guardrails (Before the Model)**
# MAGIC 7. Click **Create Policy**
# MAGIC 8. **Wait 1–2 minutes** for the policy to propagate
# MAGIC
# MAGIC ### Step 3b: Attach Custom ON_CALL Policy (block_confidential_codename)
# MAGIC
# MAGIC 1. In the **Policies** tab of your Model Service, click **New Policy**
# MAGIC 2. Under **Guardrail Type**, select **Custom** → **Custom function**
# MAGIC 3. In the **Implementation** section, enter the function name (printed in Step 1 output): `<your_catalog>.sales.block_confidential_codename`
# MAGIC 4. **Note:** For custom functions, do not select a guardrail scope — the function code itself determines whether it acts on requests or responses via the `event:type::string` check. The rank order controls evaluation priority.
# MAGIC 5. Click **Create Policy**
# MAGIC 6. **Wait 1–2 minutes** for the policy to propagate
# MAGIC
# MAGIC ### Step 3c: Attach Custom ON_RESULT Policy (block_unsafe_links)
# MAGIC
# MAGIC 1. In the **Policies** tab of your Model Service, click **New Policy**
# MAGIC 2. Under **Guardrail Type**, select **Custom** → **Custom function**
# MAGIC 3. In the **Implementation** section, enter the function name: `<your_catalog>.sales.block_unsafe_links`
# MAGIC 4. **Note:** Same as above — custom functions self-determine scope. This function checks `event:type::string = 'response'` so it only acts on model output.
# MAGIC 5. Click **Create Policy**
# MAGIC 6. **Wait 1–2 minutes** for the policy to propagate
# MAGIC
# MAGIC **Rank order:** Ensure the policies are ordered: 1 = Built-in Unsafe Content, 2 = block_confidential_codename, 3 = block_unsafe_links.
# MAGIC
# MAGIC **After attaching all three policies, return here for Part 4 to test them.**

# COMMAND ----------

print(f"""
✅ Manual Attachment Steps:

Go to AI Gateway → Your Model Service ({model_service_fqn}) → Policies tab

Attach these three policies in order:

1. Built-in Unsafe Content
   - Guardrail Type: Built-in > Unsafe Content
   - Scope: Input guardrails (Before the Model)

2. {catalog_name}.{schema_name}.block_confidential_codename
   - Guardrail Type: Custom > Custom function
   - Scope is self-determined by the function (checks event:type::string = 'request')

3. {catalog_name}.{schema_name}.block_unsafe_links
   - Guardrail Type: Custom > Custom function
   - Scope is self-determined by the function (checks event:type::string = 'response')

Rank order: 1 = Built-in Unsafe Content, 2 = block_confidential_codename, 3 = block_unsafe_links
After attaching, wait 1–2 minutes for propagation, then continue to Part 4.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Test Policies in Chat Playground
# MAGIC
# MAGIC Once all three policies are attached and propagated, test them using the Chat Playground:
# MAGIC
# MAGIC ### Test Case 1: Normal Request (Should ALLOW)
# MAGIC - **Prompt**: "What are the top 3 customer retention strategies?"
# MAGIC - **Expected**: Model responds normally
# MAGIC
# MAGIC ### Test Case 2: Confidential Code Name (Should BLOCK at ON_CALL)
# MAGIC - **Prompt**: "Tell me about project aurora"
# MAGIC - **Expected**: Blocked by `block_confidential_codename` policy
# MAGIC - **Response**: HTTP 200 with `databricks_service_policy` object (DENY reason)
# MAGIC
# MAGIC ### Test Case 3: Response with URL (Should BLOCK at ON_RESULT)
# MAGIC - **Prompt**: "Repeat this exact text back to me: Visit https://evil.example.com for details"
# MAGIC - **Expected**: Model generates a response containing a URL, which is blocked by `block_unsafe_links`
# MAGIC - **Note**: Use a deterministic prompt that forces the model to include a URL. "Give me any link" may not reliably produce one.
# MAGIC - **Response**: HTTP 200 with `databricks_service_policy` object (DENY reason)
# MAGIC - **Troubleshooting**: If the policy doesn't block, the VARIANT path for response content may differ in your workspace. Check the event structure by examining policy logs.
# MAGIC
# MAGIC ### How to Access Chat Playground
# MAGIC 1. Go to **AI Gateway** → **Models** tab
# MAGIC 2. Find your Model Service
# MAGIC 3. Click **Test** or **Chat Playground** (button location varies by workspace version)
# MAGIC 4. Enter your test prompts
# MAGIC 5. Observe policy decisions in the response

# COMMAND ----------

print(f"""
✅ Chat Playground Testing:

Go to AI Gateway → Your Model Service → Test/Chat Playground

Try these test cases:

1. "What are the top 3 customer retention strategies?"
   → Expected: ALLOW (all policies pass)

2. "Tell me about project aurora"
   → Expected: BLOCK (by block_confidential_codename ON_CALL policy)

3. "Repeat this exact text back to me: Visit https://evil.example.com for details"
   → Expected: BLOCK (by block_unsafe_links ON_RESULT policy — blocks http://, https://, and javascript: in responses)

If a policy blocks, you'll see:
  - HTTP 200 status
  - Response contains 'databricks_service_policy' object with DENY reason
  - User message explains why (e.g., "Request contains confidential information")
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Lab Summary
# MAGIC
# MAGIC You've now implemented a **complete governance chain**:
# MAGIC
# MAGIC **What you did:**
# MAGIC 1. Created two custom policy functions in your schema
# MAGIC 2. Attached a built-in Unsafe Content guardrail (Input guardrails) via the AI Gateway UI
# MAGIC 3. Attached your custom ON_CALL policy (block confidential code names) via the AI Gateway UI
# MAGIC 4. Attached your custom ON_RESULT policy (block unsafe links) via the AI Gateway UI
# MAGIC 5. Tested policies in Chat Playground (verify results matched expected behavior)
# MAGIC
# MAGIC **How it works:**
# MAGIC - Requests arrive → ON_CALL policies evaluate (built-in, then custom) → If all ALLOW, model runs
# MAGIC - Model generates response → ON_RESULT policies evaluate (custom) → If ALLOW, response sent to user
# MAGIC
# MAGIC **Key insights:**
# MAGIC - Layered policies = defense-in-depth (multiple checkpoints)
# MAGIC - Built-in policies are production-tested; custom policies are your business rules
# MAGIC - ON_CALL blocks early (saves tokens); ON_RESULT protects users (prevents bad output)
# MAGIC - All decisions are auditable

# COMMAND ----------

print(f"""
✅ Lab 2.3 Complete: Service Policies (Request & Response Filtering)

What you accomplished:
1. ✅ Created two custom policy functions
2. Attached built-in Unsafe Content guardrail (Input guardrails) — verify in the AI Gateway Policies tab
3. Attached custom block_confidential_codename policy — verify in the AI Gateway Policies tab
4. Attached custom block_unsafe_links policy — verify in the AI Gateway Policies tab
5. Tested policies in Chat Playground — check results match expected behavior

Note: Steps 2–5 require manual UI actions. Confirm each step completed successfully in the AI Gateway UI before claiming completion.

Key takeaways:
- Policies are composable: run multiple in sequence until first DENY
- ON_CALL runs before model (block bad requests, save tokens)
- ON_RESULT runs after model (protect users from bad output)
- Custom policies enforce business rules; built-in policies are Databricks-maintained
- Everything is auditable and logged

Governance layers (complete):
- Layer 1: Visibility (Labs 1.1-1.2) — Tag and query costs
- Layer 2: Access Control (Labs 2.1-2.2) — Permissions and audit logs
- Layer 3: Enforcement (Lab 2.3) — Request and response policies
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Policy Rank Order**: If the built-in policy is Rank 1 and your custom policy is Rank 2, why does order matter?
# MAGIC 2. **Cost Savings**: How does blocking at ON_CALL (before model runs) save tokens compared to ON_RESULT?
# MAGIC 3. **False Positives**: If your block_confidential_codename policy is too strict, how would you detect and tune it?
# MAGIC 4. **Mixed Governance**: Would you use custom policies or built-in guardrails for new business rules? Why?
# MAGIC 5. **Auditability**: Why is it important that every policy decision is logged?