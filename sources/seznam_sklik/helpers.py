"""Seznam Sklik source helpers"""
from typing import Iterator, Dict, Any, List, Optional

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from sklik import SklikApi, create_report
from sklik.object import Account

from .settings import SKLIK_DATA_FORMAT


def get_start_date(
        incremental_start_date: dlt.sources.incremental[str],
        attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    """
    Get the start date for incremental loading of Seznam Sklik stats data.
    """
    start_date: pendulum.DateTime = ensure_pendulum_datetime(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

    # lag the incremental start date by attribution window lag
    incremental_start_date.start_value = start_date.isoformat()
    return start_date


def validate_and_format_dates(
        start_date: str,
        end_date: str
) -> tuple[str, str]:
    try:
        start = pendulum.parse(start_date)
        end = pendulum.parse(end_date)

        if start >= end:
            start, end = end, start

        formatted_start = start.format(SKLIK_DATA_FORMAT)
        formatted_end = end.format(SKLIK_DATA_FORMAT)

        return formatted_start, formatted_end

    except Exception as e:
        raise ValueError(f"Error processing dates: {str(e)}")


def get_filtered_accounts(
        sklik_api: SklikApi,
        account_id: Optional[int] = None,
        access_type: str = "rw",
) -> Iterator[TDataItem]:
    response = sklik_api.call("client", "get")

    main_account = response["user"]
    foreign_accounts = response.get("foreignAccounts", [])
    all_accounts = [main_account] + foreign_accounts

    for account in all_accounts:
        if (account_id is None or account_id == account["userId"]) and access_type in account.get("access", "rw"):
            yield account


def get_setting_data(
        sklik_api: SklikApi,
        account: TDataItem,
        service: str,
        fields: List[str]
) -> Iterator[TDataItem]:
    account_id = account["userId"]

    response = sklik_api.call(
        service=service,
        method="list",
        args=[{"userId": account_id}, {}, {"limit": 5000, "offset": 0, "displayColumns": fields}],
    )

    for row in response[service]:
        row["account_Id"] = account_id
        row["account_name"] = account["username"]
        yield row


def get_stats_data(
        sklik_api: SklikApi,
        account: TDataItem,
        level: str,
        start_date: str,
        end_date: str,
        fields: List[str],
        granularity: str,
        restriction_filter: Dict[str, Any],
) -> Iterator[TDataItem]:
    since, until = validate_and_format_dates(start_date, end_date)

    sklik_account = Account(account["userId"], api=sklik_api)
    report = create_report(
        account=sklik_account,
        service=level,
        since=since,
        until=until,
        fields=fields,
        granularity=granularity,
        restriction_filter=restriction_filter,
    )

    for stats in report:
        if stats["date"]:
            stats["date"] = pendulum.parse(str(stats["date"])).to_date_string()
        stats["account_id"] = account["userId"]
        stats["account_name"] = account["username"]
        yield stats
