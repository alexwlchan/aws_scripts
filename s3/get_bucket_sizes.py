#!/usr/bin/env python
"""
Prints some information about the amount of information you're storing in each
bucket in your account.

Useful if you know you're spending a lot on storage *somewhere*, but you're not
sure which bucket it is.

"""

import csv
import datetime
import uuid

import boto3
import humanize

from common import get_buckets


def get_sizes_for_bucket(cloudwatch_client, *, bucket_name):
    """
    Given a bucket name, return a rough estimate for the bytes in this bucket
    per storage class.
    """
    # CloudWatch Metrics has a 'BucketSizeBytes' metric, with one for each storage
    # class.  First use list_metrics to find out what those metrics are (i.e. which
    # storage classes are we using in this bucket?).
    storage_class_dimensions = []

    paginator = cloudwatch_client.get_paginator("list_metrics")

    for page in paginator.paginate(
        Namespace="AWS/S3",
        MetricName="BucketSizeBytes",
        Dimensions=[{"Name": "BucketName", "Value": bucket_name}],
    ):
        for metric in page["Metrics"]:
            storage_class_dimensions.append(metric["Dimensions"])

    storage_classes = {
        next(
            dim["Value"] for dim in dimensions if dim["Name"] == "StorageType"
        ): dimensions
        for dimensions in storage_class_dimensions
    }

    # Now we know what storage classes are in use in this bucket, let's go ahead
    # and fetch them.  Note: the S3 Metrics aren't updated that frequently, so
    # we need to pick a fairly long period.
    storage_class_queries = [
        {
            "Id": name.lower(),
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/S3",
                    "MetricName": "BucketSizeBytes",
                    "Dimensions": dimensions,
                },
                "Period": 3 * 24 * 60 * 60,
                "Stat": "Average",
            },
        }
        for name, dimensions in storage_classes.items()
    ]

    number_of_objects_query = [
        {
            "Id": "number_of_objects",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/S3",
                    "MetricName": "NumberOfObjects",
                    "Dimensions": [
                        {"Name": "BucketName", "Value": bucket_name},
                        {"Name": "StorageType", "Value": "AllStorageTypes"},
                    ],
                },
                "Period": 7 * 24 * 60 * 60,
                "Stat": "Average",
            },
        }
    ]

    resp = cloudwatch_client.get_metric_data(
        MetricDataQueries=storage_class_queries + number_of_objects_query,
        StartTime=datetime.datetime.now() - datetime.timedelta(days=14),
        EndTime=datetime.datetime.now(),
    )

    # Finally, tidy up the data into a format that's a bit easier to deal
    # with in the calling code.
    rv = {"bucket_name": bucket_name}

    for metric_result in resp["MetricDataResults"]:

        # The number of objects we pass straight through
        if metric_result["Id"] == "number_of_objects":
            try:
                rv["number_of_objects"] = int(metric_result["Values"][-1])
            except IndexError:
                rv["number_of_objects"] = 0

        else:
            # For storage classes, we include the raw number of bytes, and a
            # human-readable storage value.
            storage_class_name = next(
                class_name
                for class_name in storage_classes
                if class_name.lower() == metric_result["Id"]
            )

            rv[storage_class_name] = int(metric_result["Values"][-1])
            rv[f"{storage_class_name} (human-readable)"] = humanize.naturalsize(
                metric_result["Values"][-1]
            )

    return rv


if __name__ == "__main__":
    s3_client = boto3.client("s3")
    cloudwatch_client = boto3.client("cloudwatch")

    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_name = f"bucket_size_{now}.csv"

    with open(csv_name, "w") as outfile:
        storage_names = [
            "StandardStorage",
            "StandardIAStorage",
            "StandardIASizeOverhead",
            "ReducedRedundancyStorage",
            "GlacierStorage",
            "GlacierObjectOverhead",
            "GlacierS3ObjectOverhead",
            "DeepArchiveStorage",
            "DeepArchiveObjectOverhead",
            "DeepArchiveS3ObjectOverhead",
            "DeepArchiveStagingStorage",
        ]

        writer = csv.DictWriter(
            outfile,
            fieldnames=[
                "bucket_name",
                "number_of_objects"
            ] + storage_names + [f"{name} (human-readable)" for name in storage_names],
        )
        writer.writeheader()

        for bucket in get_buckets(s3_client):
            row = get_sizes_for_bucket(cloudwatch_client, bucket_name=bucket["Name"])
            writer.writerow(row)

    print(f"✨ Written information about your S3 stats to {csv_name} ✨")
