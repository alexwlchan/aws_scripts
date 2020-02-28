def get_log_groups(logs_client):
    """
    Generates every log group in the current AWS account.
    """
    paginator = logs_client.get_paginator("describe_log_groups")

    for page in paginator.paginate():
        yield from page["logGroups"]


def get_log_streams(logs_client, *, log_group_name):
    """
    Generates every log stream for a given log group.

    Log streams are ordered by their last event time: streams that logged
    more recently appear higher.
    """
    paginator = logs_client.get_paginator("describe_log_streams")

    for page in paginator.paginate(
        logGroupName=log_group_name, orderBy="LastEventTime", descending=True
    ):
        yield from page["logStreams"]
