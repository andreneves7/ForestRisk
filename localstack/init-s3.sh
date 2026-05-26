#!/bin/bash
echo "A criar buckets S3 no LocalStack..."
awslocal s3 mb s3://forest-risk-datalake --region eu-west-1
awslocal s3 mb s3://forest-risk-models   --region eu-west-1
awslocal s3api put-bucket-versioning \
  --bucket forest-risk-datalake \
  --versioning-configuration Status=Enabled
echo "Buckets criados:"
awslocal s3 ls
