#!/usr/bin/env python
"""
This script finds secrets which haven't been accessed in the last year,
and which might not be in use any more.

Just because a secret is old doesn't mean it can be deleted -- e.g. it might
be needed for a disaster recovery procedure -- but it hasn't been checked
in a while. so it may need testing or checking that it still works.

This is meant to start conversations about long-forgotten secrets.

"""

import datetime

import boto3


def list_secrets(sess):
    client = sess.client("secretsmanager")

    for page in client.get_paginator("list_secrets").paginate():
        yield from page["SecretList"]


if __name__ == "__main__":
    sess = boto3.Session()

    now = datetime.datetime.utcnow()

    for secret in list_secrets(sess):
        try:
            last_accessed_date = secret["LastAccessedDate"]
        except KeyError:
            print(secret["Name"], "(never)")
        else:
            delta = now - last_accessed_date.replace(tzinfo=None)
            if delta.days > 365:
                print(secret["Name"], last_accessed_date.date().isoformat())
