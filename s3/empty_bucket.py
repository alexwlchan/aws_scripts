#!/usr/bin/env python

import itertools

import boto3
import humanize
import inquirer
import termcolor

from common import get_buckets, get_objects, get_object_versions, has_versioning_enabled


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def _for_delete_objects(obj):
    return {key: value for key, value in obj.items() if key in {"Key", "VersionId"}}


if __name__ == "__main__":
    s3_client = boto3.client("s3")

    all_buckets = list(get_buckets(s3_client))

    questions = [
        inquirer.List(
            "bucket",
            message="Which bucket do you want to empty?",
            choices=[bucket["Name"] for bucket in list(get_buckets(s3_client))],
        ),
        inquirer.Text("prefix", message="What prefix do you want to delete?"),
    ]

    answers = inquirer.prompt(questions)

    bucket = answers["bucket"]
    prefix = answers["prefix"]

    if has_versioning_enabled(s3_client, bucket=bucket):
        questions = [
            inquirer.List(
                "process",
                message=f"{answers['bucket']} is versioned. When deleting",
                choices=["soft delete", "permanent delete"],
            )
        ]

        answers = inquirer.prompt(questions)

        if answers["process"] == "soft delete":
            get_objects_to_delete = get_objects
        else:
            get_objects_to_delete = get_object_versions

    else:
        get_objects_to_delete = get_objects

    total_deleted = 0
    total_size = 0

    for objects in chunked_iterable(
        get_objects_to_delete(s3_client, bucket=bucket, prefix=prefix), size=1000
    ):
        resp = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [_for_delete_objects(s3_obj) for s3_obj in objects]},
        )

        total_deleted += len(objects)
        total_size += sum(s3_obj.get("Size", 0) for s3_obj in objects)

        print(
            "Deleted %s objects, totalling %s"
            % (
                termcolor.colored(humanize.intcomma(total_deleted), "green"),
                termcolor.colored(humanize.naturalsize(total_size), "green"),
            ),
        )
