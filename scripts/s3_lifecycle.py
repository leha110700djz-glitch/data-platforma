"""
S3 lifecycle rules + versioning automation via boto3.
- bronze: GLACIER_IR after 30 days, DEEP_ARCHIVE after 180 days
- silver: STANDARD_IA after 60 days
- gold:  keep in STANDARD (hot) + 7 versions non-current
- bucket versioning enabled + MFA delete for prod buckets
"""
from __future__ import annotations

import argparse
import json
import logging
from typing import Any

import boto3

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def apply_lifecycle(client: Any, bucket: str, layer: str) -> None:
    """Apply layer-specific lifecycle policy to bucket."""
    rules = {
        "bronze": [
            {"ID": "bronze-to-glacier-ir", "Status": "Enabled",
             "Filter": {"Prefix": "bronze/"},
             "Transitions": [
                 {"Days": 30,  "StorageClass": "GLACIER_IR"},
                 {"Days": 180, "StorageClass": "DEEP_ARCHIVE"},
             ],
             "Expiration": {"Days": 365}},
        ],
        "silver": [
            {"ID": "silver-to-ia", "Status": "Enabled",
             "Filter": {"Prefix": "silver/"},
             "Transitions": [{"Days": 60, "StorageClass": "STANDARD_IA"}],
             "NoncurrentVersionExpiration": {"NoncurrentDays": 30}},
        ],
        "gold": [
            {"ID": "gold-versions", "Status": "Enabled",
             "Filter": {"Prefix": "gold/"},
             "NoncurrentVersionExpiration": {"NoncurrentDays": 7, "NewerNoncurrentVersions": 3}},
        ],
    }[layer]

    client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": rules},
    )
    log.info("Applied lifecycle '%s' on bucket %s: %s", layer, bucket, json.dumps(rules, indent=2))


def enable_versioning(client: Any, bucket: str, mfa_delete: bool = False) -> None:
    """Enable bucket versioning (and optionally MFA delete for prod)."""
    config = {"Status": "Enabled"}
    if mfa_delete:
        config["MFADelete"] = "Enabled"
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration=config)
    log.info("Versioning enabled on %s (mfa_delete=%s)", bucket, mfa_delete)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default="https://s3.data-platform.internal")
    p.add_argument("--bucket", required=True)
    p.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    p.add_argument("--mfa-delete", action="store_true", help="Enable MFA delete (prod only)")
    args = p.parse_args()

    s3 = boto3.client("s3", endpoint_url=args.endpoint)
    enable_versioning(s3, args.bucket, mfa_delete=args.mfa_delete)
    apply_lifecycle(s3, args.bucket, args.layer)


if __name__ == "__main__":
    main()
