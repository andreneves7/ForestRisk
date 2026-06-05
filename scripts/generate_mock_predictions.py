"""
Generate mock ML predictions for Portuguese forest risk regions and upload to S3.

Usage (from project root):
    python scripts/generate_mock_predictions.py

Required env vars:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION (default: eu-west-1)

Output:
    data/mock/mock_predictions.csv  (50 rows, seeded at 42)
    s3://forest-risk-datalake/mock/mock_predictions.csv
"""

import csv
import os
import random

import boto3

REGIOES = [
    ("PT-AVR-001", "Aveiro Norte"), ("PT-AVR-002", "Aveiro Sul"),
    ("PT-BEJ-001", "Beja Interior"), ("PT-BEJ-002", "Beja Litoral"),
    ("PT-BRG-001", "Braga Norte"), ("PT-BRG-002", "Braga Sul"),
    ("PT-BGR-001", "Bragança Este"), ("PT-BGR-002", "Bragança Oeste"),
    ("PT-CBR-001", "Castelo Branco Norte"), ("PT-CBR-002", "Castelo Branco Sul"),
    ("PT-COI-001", "Coimbra Norte"), ("PT-COI-002", "Coimbra Sul"),
    ("PT-EVR-001", "Évora Norte"), ("PT-EVR-002", "Évora Sul"),
    ("PT-FAR-001", "Faro Litoral"), ("PT-FAR-002", "Faro Interior"),
    ("PT-GRD-001", "Guarda Norte"), ("PT-GRD-002", "Guarda Sul"),
    ("PT-LEI-001", "Leiria Norte"), ("PT-LEI-002", "Leiria Sul"),
    ("PT-LIS-001", "Lisboa Norte"), ("PT-LIS-002", "Lisboa Sul"),
    ("PT-PTG-001", "Portalegre Norte"), ("PT-PTG-002", "Portalegre Sul"),
    ("PT-PRT-001", "Porto Norte"), ("PT-PRT-002", "Porto Sul"),
    ("PT-STB-001", "Setúbal Norte"), ("PT-STB-002", "Setúbal Sul"),
    ("PT-VCT-001", "Viana do Castelo Norte"), ("PT-VCT-002", "Viana do Castelo Sul"),
    ("PT-VRL-001", "Vila Real Norte"), ("PT-VRL-002", "Vila Real Sul"),
    ("PT-VSU-001", "Viseu Norte"), ("PT-VSU-002", "Viseu Sul"),
    ("PT-MDE-001", "Madeira Este"), ("PT-AZO-001", "Açores Central"),
    ("PT-SNT-001", "Santarém Norte"), ("PT-SNT-002", "Santarém Sul"),
    ("PT-STP-001", "Setúbal Península"), ("PT-OEI-001", "Oeste Interior"),
    ("PT-ALG-001", "Algarve Este"), ("PT-ALG-002", "Algarve Oeste"),
    ("PT-ALE-001", "Alentejo Central"), ("PT-ALE-002", "Alentejo Litoral"),
    ("PT-TRS-001", "Trás-os-Montes Norte"), ("PT-TRS-002", "Trás-os-Montes Sul"),
    ("PT-DRO-001", "Douro Norte"), ("PT-DRO-002", "Douro Sul"),
    ("PT-MNH-001", "Minho Norte"), ("PT-MNH-002", "Minho Sul"),
]

FIELDNAMES = ["grid_id", "regiao", "predicted_risk_score", "confidence", "prediction_date", "model_version"]

if __name__ == "__main__":
    prediction_date = "2026-06-05"  # fixture date — matches committed CSV

    random.seed(42)

    rows = []
    for grid_id, regiao in REGIOES:
        risk = round(random.uniform(5, 98), 1)
        confidence = round(random.uniform(60, 95), 1)
        rows.append({
            "grid_id": grid_id,
            "regiao": regiao,
            "predicted_risk_score": risk,
            "confidence": confidence,
            "prediction_date": prediction_date,
            "model_version": "v0.1-mock",
        })

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
    csv_path = os.path.join(PROJECT_ROOT, "data", "mock", "mock_predictions.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV gerado: {csv_path} ({len(rows)} linhas)")

    # Upload to S3
    bucket = "forest-risk-datalake"
    s3_key = "mock/mock_predictions.csv"

    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-1"))

    try:
        s3.upload_file(csv_path, bucket, s3_key)
        print(f"Uploaded to s3://{bucket}/{s3_key}")
    except Exception as exc:
        print(f"S3 upload failed: {exc}")
        print(f"Local CSV is still available at: {csv_path}")
        raise SystemExit(1)
