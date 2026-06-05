# AWS IAM Policies

## forestrisk-user-policy.json

Least-privilege policy for the `ForestRiskUser` IAM user.

**Replaces:** AmazonAthenaFullAccess + AWSGlueConsoleFullAccess (overly broad)

**Grants:**
- S3: read/write on `forest-risk-datalake`, `forest-risk-models`, `forest-risk-athena-results` only
- Athena: query execution in any workgroup (required for ODBC)
- Glue: read/write on `forest_risk` database only (no other databases)

**How to apply:**
1. IAM console → Users → ForestRiskUser → Permissions
2. Remove: AmazonAthenaFullAccess, AWSGlueConsoleFullAccess
3. Add permissions → Attach policies → Create inline policy
4. Paste JSON from this file → name: `ForestRiskLeastPrivilege`

**Test after applying:**
- Power BI refresh still works
- `aws s3 ls s3://forest-risk-datalake/` works
- `aws s3 ls s3://other-bucket/` returns AccessDenied (expected)
