# CloudTrail Audit Setup

## Configuration

| Setting | Value |
|---------|-------|
| Trail name | forest-risk-audit |
| Region | eu-west-1 |
| Log destination | s3://forest-risk-logs/ |
| Management events | Read + Write |
| S3 data events | Read + Write on forest-risk-datalake |

## Steps to create the trail

1. Open AWS CloudTrail console → eu-west-1 → Create trail
2. Trail name: `forest-risk-audit`
3. Storage location: Create new S3 bucket → name: `forest-risk-logs`
4. Log file SSE-KMS encryption: disabled (SSE-S3 default is sufficient)
5. CloudWatch Logs: skip (not required for academic project)
6. Under "Events":
   - Management events: Read + Write ✅
   - Exclude AWS KMS events: checked (reduces noise)
7. Under "Data events":
   - Add S3 data event → select `forest-risk-datalake` → Read + Write
8. Click Create trail

## Purpose

Audit trail for S3 and Athena access — required for the security section of the project report.
CloudTrail records every API call: who accessed which file, from where, when.

## Retention

CloudTrail delivers logs to `forest-risk-logs` within ~15 minutes.
Logs are stored indefinitely (no lifecycle rule configured for this academic project).

## Verification

After setup, run any Athena query from Power BI or the console, then check:
```
s3://forest-risk-logs/AWSLogs/<account-id>/CloudTrail/eu-west-1/<year>/<month>/<day>/
```
A `.json.gz` log file should appear within 15 minutes containing the Athena API call.
