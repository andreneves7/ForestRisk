# S3 Security Hardening Checklist

| Bucket | Versioning | Block Public Access | SSE-S3 |
|--------|-----------|---------------------|--------|
| forest-risk-datalake | ✅ Enabled | ✅ All ON | ✅ Default |
| forest-risk-models | ✅ Enabled | ✅ All ON | ✅ Default |
| forest-risk-athena-results | — | ✅ All ON | ✅ Default |

## Steps to apply

### Enable S3 versioning on forest-risk-datalake
1. AWS S3 console → `forest-risk-datalake` → Properties tab
2. Bucket Versioning → Enable → Save

### Verify Block Public Access
1. S3 console → each bucket → Permissions tab
2. Block public access → all 4 options must be ON
3. Repeat for `forest-risk-models` and `forest-risk-athena-results`

### Verify SSE-S3 encryption (default)
1. S3 console → each bucket → Properties tab
2. Default encryption → should show "SSE-S3 (AES-256)"
3. If not enabled: Edit → Enable → SSE-S3 → Save

## Notes
- `forest-risk-athena-results`: versioning not needed (Athena result files are ephemeral query outputs)
- `forest-risk-models`: enable versioning to protect MLflow model artefacts from accidental deletion
- All buckets must be in eu-west-1 (Ireland) — verify region in S3 console
