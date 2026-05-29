#!/bin/bash
set -euo pipefail

echo "A criar buckets S3 no LocalStack..."

awslocal s3 mb s3://forest-risk-datalake --region eu-west-1 2>/dev/null || true
awslocal s3 mb s3://forest-risk-models   --region eu-west-1 2>/dev/null || true

awslocal s3api put-bucket-versioning \
  --bucket forest-risk-datalake \
  --versioning-configuration Status=Enabled

awslocal s3api put-bucket-versioning \
  --bucket forest-risk-models \
  --versioning-configuration Status=Enabled

echo "Buckets criados com versioning:"
awslocal s3 ls
