#!/usr/bin/env python
"""
When I don't know what I'm looking for in a CloudWatch log, it can be easier
to dump the whole thing to a local file, and use tools like grep to pick through
the logs -- the CloudWatch console is less than fun.

This script gets every event from a log group and prints it to stdout.

See https://alexwlchan.net/2017/11/fetching-cloudwatch-logs/ for an explanation
of how this script works.

"""

import boto3
import click
import maya


def get_log_events(logs_client, log_group, start_time, end_time):
    """Generate all the log events from a CloudWatch group.

    :param logs_client: An instance of boto3.client("logs")
    :param log_group: Name of the CloudWatch log group.
    :param start_time: Only fetch events with a timestamp after this time.
        Expressed as the number of milliseconds after midnight Jan 1 1970.
    :param end_time: Only fetch events with a timestamp before this time.
        Expressed as the number of milliseconds after midnight Jan 1 1970.

    """
    kwargs = {
        "logGroupName": log_group,
        "limit": 10000,
    }

    if start_time is not None:
        kwargs["startTime"] = milliseconds_since_epoch(start_time)
    if end_time is not None:
        kwargs["endTime"] = milliseconds_since_epoch(end_time)

    paginator = logs_client.get_paginator("filter_log_events")
    for page in paginator.paginate(**kwargs):
        yield from page["events"]


def milliseconds_since_epoch(time_string):
    """
    The CloudWatch API measures time as milliseconds since the epoch (1 Jan 1970).
    """
    dt = maya.when(time_string)
    seconds = dt.epoch
    return seconds * 1000


@click.command()
@click.argument("log_group_name")
@click.option("--start", help="Only print events with a timestamp after this time")
@click.option("--end", help="Only print events with a timestamp before this time")
def main(log_group_name, start, end):
    """
    Print log messages from a CloudWatch log group.
    """
    logs_client = boto3.client("logs")

    logs = get_log_events(
        logs_client=logs_client,
        log_group=log_group_name,
        start_time=start,
        end_time=end,
    )

    for event in logs:
        print(event["message"].rstrip())


if __name__ == "__main__":
    main()
