#!/usr/bin/env python
"""
We run Archivematica [1] in AWS, and we send logs to CloudWatch.

The Archivematica logs are very chatty until an exception is thrown, at which
point the worker prints a traceback, then it stops and there's no more output.

If this was writing to a local file, we could use tail [2] to read the last
few lines of log and see the exception.  This is an attempt to mimic that for
CloudWatch: it tries to get the last 10 seconds of output, which is usually
enough to debug the issue.

[1]: https://www.archivematica.org/en/
[2]: https://linux.die.net/man/1/tail

Usage:

    tail_log_stream.py [<LOG_GROUP_NAME>]

If you don't give a log group name, you will be asked to choose a log group
from all the log groups in your AWS account.

"""

import datetime
import sys

import boto3
import humanize
import inquirer
import iterfzf

from common import get_log_groups, get_log_streams


def get_log_group_names(logs_client):
    for log_group in get_log_groups(logs_client=logs_client):
        yield log_group["logGroupName"]


def get_last_ten_seconds_from_stream(logs_client, *, log_group_name, log_stream):
    # This timestamp is milliseconds since the epoch, so to wind back 10 seconds
    # is 10 * 1000 milliseconds.
    start_time = log_stream["lastEventTimestamp"] - 10 * 1000

    paginator = logs_client.get_paginator("filter_log_events")

    for page in paginator.paginate(
        logGroupName=log_group_name,
        logStreamNames=[log_stream["logStreamName"]],
        startTime=start_time,
    ):
        yield from page["events"]


if __name__ == "__main__":
    logs_client = boto3.client("logs")

    try:
        log_group_name = sys.argv[1]
    except IndexError:
        log_group_name = iterfzf.iterfzf(
            get_log_group_names(logs_client),
            prompt="What log group do you want to tail? ",
        )

    all_streams = get_log_streams(logs_client, log_group_name=log_group_name)

    choices = {}
    for _ in range(5):
        stream = next(all_streams)
        dt = datetime.datetime.fromtimestamp(stream["lastEventTimestamp"] / 1000)
        name = stream["logStreamName"]
        choices[f"{name} ({humanize.naturaltime(dt)})"] = stream

    questions = [
        inquirer.List(
            "log_stream_id",
            message="Which log stream would you like to tail?",
            choices=choices,
        )
    ]

    answers = inquirer.prompt(questions)

    log_stream = choices[answers["log_stream_id"]]

    for event in get_last_ten_seconds_from_stream(
        logs_client, log_group_name=log_group_name, log_stream=log_stream
    ):
        print(event["message"])
