#!/usr/bin/env python

def get_objects(s3_client, *, bucket, prefix):
    """
    Generates every object in an S3 bucket.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page["Contents"]


def get_object_versions(s3_client, *, bucket, prefix):
    """
    Generates every version of an object in an S3 bucket.
    """
    paginator = s3_client.get_paginator("list_object_versions")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for key in ("Versions", "DeleteMarkers"):
            try:
                yield from page[key]
            except KeyError:
                pass


def get_buckets(s3_client):
    """
    Generates every bucket in this AWS account.
    """
    resp = s3_client.list_buckets()
    yield from resp["Buckets"]


def has_versioning_enabled(s3_client, *, bucket):
    """
    Does this bucket have versioning enabled?
    """
    resp = s3_client.get_bucket_versioning(Bucket=bucket)
    return resp["Status"] == "Enabled"
