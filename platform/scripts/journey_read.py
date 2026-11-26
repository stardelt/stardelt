#!/usr/bin/env python3
"""Read one Iceberg table straight from an S3 gateway: no catalog, no stardelt (#1806 X1)."""
import argparse
import sys


def newest_metadata(client, bucket, needle):
    """The key of the newest <needle>*.metadata.json under <bucket>, or None."""
    newest = None
    token = None
    while True:
        kwargs = {"Bucket": bucket}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for row in page.get("Contents") or []:
            key = row["Key"]
            if needle in key and key.endswith(".metadata.json"):
                # SeaweedFS answers LastModified at second granularity, so two commits of the same
                # second tie; the zero-padded version prefix in the key breaks the tie.
                if newest is None or (row["LastModified"], key) > (newest[1], newest[0]):
                    newest = (key, row["LastModified"])
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return newest[0] if newest else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="http://127.0.0.1:<port>")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--needle", required=True, help="a path fragment of the table, e.g. /e2e/exit_test/metadata/")
    parser.add_argument("--access-key-file", required=True)
    parser.add_argument("--secret-key-file", required=True)
    parser.add_argument("--rows", type=int, required=True)
    args = parser.parse_args(argv)

    import boto3
    from botocore.config import Config
    from pyiceberg.table import StaticTable

    access = open(args.access_key_file, encoding="utf-8").read().strip()
    secret = open(args.secret_key_file, encoding="utf-8").read().strip()
    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    key = newest_metadata(client, args.bucket, args.needle)
    if key is None:
        print("no metadata file under s3://%s for %s" % (args.bucket, args.needle))
        return 1
    table = StaticTable.from_metadata(
        "s3://%s/%s" % (args.bucket, key),
        properties={
            "s3.endpoint": args.endpoint,
            "s3.access-key-id": access,
            "s3.secret-access-key": secret,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    rows = table.scan().to_arrow().num_rows
    if rows != args.rows:
        print("%s has %d rows, not %d" % (key, rows, args.rows))
        return 1
    print("%d rows from %s (pyiceberg, no catalog)" % (rows, key))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
