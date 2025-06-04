import copy
from typing import Iterator, List

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem
from dlt.extract import DltResource
from sklik import SklikApi

from .columns import (
    SKLIK_ACCOUNT_SETTING_COLUMNS,
    SKLIK_CAMPAIGNS_SETTINGS_COLUMNS,
    SKLIK_GROUPS_SETTINGS_COLUMNS,
    SKLIK_GROUPS_STATS_COLUMNS,
    SKLIK_ADS_STATS_COLUMNS,
    SKLIK_BANNERS_STATS_COLUMNS,
    SKLIK_QUERIES_STATS_COLUMNS,
    SKLIK_RETARGETING_STATS_COLUMNS,
    SKLIK_GROUPS_DEVICE_STATS_COLUMNS,
)
from .helpers import get_filtered_accounts, get_setting_data, get_start_date, get_stats_data
from .settings import (
    STATS_PRIMARY_KEY,
    DEFAULT_CAMPAIGN_SETTINGS_FIELDS,
    DEFAULT_GROUP_SETTINGS_FIELDS,
    DEFAULT_RESTRICTION_FILTER,
    DEFAULT_GROUP_STATS_FIELDS,
    DEFAULT_AD_STATS_FIELDS,
    DEFAULT_AD_SETTINGS_FIELDS,
    DEFAULT_BANNER_SETTINGS_FIELDS,
    DEFAULT_BANNER_STATS_FIELDS,
    DEFAULT_QUERIES_STATS_FIELDS,
    DEFAULT_RETARGETING_STATS_FIELDS,
)


@dlt.source(name="seznam_sklik")
def unified_sklik_source(access_token: str = dlt.secrets.value) -> List[DltResource]:
    sklik_api = SklikApi.init(access_token)

    @dlt.resource(
        name="accounts",
        table_name="accounts",
        write_disposition="replace",
        columns=SKLIK_ACCOUNT_SETTING_COLUMNS,
    )
    def accounts(access_type: str = "rw") -> Iterator[TDataItem]:
        yield get_filtered_accounts(sklik_api, access_type=access_type)

    @dlt.transformer(
        data_from=accounts,
        name="campaigns",
        table_name="campaigns",
        write_disposition="replace",
        columns=SKLIK_CAMPAIGNS_SETTINGS_COLUMNS,
    )
    def campaigns(account: TDataItem) -> Iterator[TDataItem]:
        yield get_setting_data(sklik_api, account, "campaigns", list(DEFAULT_CAMPAIGN_SETTINGS_FIELDS))

    @dlt.transformer(
        data_from=accounts,
        name="groups",
        table_name="groups",
        write_disposition="replace",
        columns=SKLIK_GROUPS_SETTINGS_COLUMNS,
    )
    def groups(account: TDataItem) -> Iterator[TDataItem]:
        yield get_setting_data(sklik_api, account, "groups", list(DEFAULT_GROUP_SETTINGS_FIELDS))

    return [
        accounts,
        accounts | campaigns,
        accounts | groups,
    ]


@dlt.source(name="seznam_sklik")
def unified_sklik_stat_source(
        access_token: str = dlt.secrets.value,
        initial_load_past_days: int = 28,
        attribution_window_days_lag: int = 7,
        queries_refresh_days: int = 28,
) -> List[DltResource]:
    sklik_api = SklikApi.init(access_token)

    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(name="stats", write_disposition="skip")
    def accounts(access_type: str = "rw") -> Iterator[TDataItem]:
        yield get_filtered_accounts(sklik_api, access_type=access_type)

    @dlt.transformer(
        data_from=accounts,
        name="groups_stats",
        table_name="groups_stats",
        write_disposition="merge",
        merge_key=STATS_PRIMARY_KEY,
        columns=SKLIK_GROUPS_DEVICE_STATS_COLUMNS,
    )
    def groups_stats(
            account: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        fields = list(DEFAULT_GROUP_STATS_FIELDS)
        restriction_filter = copy.deepcopy(DEFAULT_RESTRICTION_FILTER)
        restriction_filter["deviceType"] = ["deviceDesktop", "devicePhone", "deviceTablet", "deviceOther"]

        yield get_stats_data(
            sklik_api, account, "groups", start_date, end_date, fields, "daily", restriction_filter
        )

    @dlt.transformer(
        data_from=accounts,
        name="retargeting_stats",
        table_name="retargeting_stats",
        write_disposition="merge",
        merge_key=STATS_PRIMARY_KEY,
        columns=SKLIK_RETARGETING_STATS_COLUMNS,
    )
    def retargeting_stats(
            account: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        fields = list(DEFAULT_RETARGETING_STATS_FIELDS)
        restriction_filter = copy.deepcopy(DEFAULT_RESTRICTION_FILTER)

        yield get_stats_data(
            sklik_api, account, "retargeting", start_date, end_date, fields, "daily", restriction_filter
        )

    @dlt.transformer(
        data_from=accounts,
        name="ads_stats",
        table_name="ads_stats",
        write_disposition="merge",
        merge_key=STATS_PRIMARY_KEY,
        columns=SKLIK_ADS_STATS_COLUMNS,
    )
    def ads_stats(
            account: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        fields = list(set(DEFAULT_AD_SETTINGS_FIELDS + DEFAULT_AD_STATS_FIELDS))
        restriction_filter = copy.deepcopy(DEFAULT_RESTRICTION_FILTER)

        yield get_stats_data(
            sklik_api, account, "ads", start_date, end_date, fields, "daily", restriction_filter
        )

    @dlt.transformer(
        data_from=accounts,
        name="banners_stats",
        table_name="banners_stats",
        write_disposition="merge",
        merge_key=STATS_PRIMARY_KEY,
        columns=SKLIK_BANNERS_STATS_COLUMNS,
    )
    def banners_stats(
            account: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        fields = list(set(DEFAULT_BANNER_SETTINGS_FIELDS + DEFAULT_BANNER_STATS_FIELDS))
        restriction_filter = copy.deepcopy(DEFAULT_RESTRICTION_FILTER)

        yield get_stats_data(
            sklik_api, account, "banners", start_date, end_date, fields, "daily", restriction_filter
        )

    @dlt.transformer(
        data_from=accounts,
        name="queries_stats",
        table_name="queries_stats",
        write_disposition="replace",
        columns=SKLIK_QUERIES_STATS_COLUMNS
    )
    def queries_stats(account: TDataItem) -> Iterator[TDataItem]:
        start_date = pendulum.yesterday().subtract(days=queries_refresh_days).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        fields = list(DEFAULT_QUERIES_STATS_FIELDS)
        restriction_filter = copy.deepcopy(DEFAULT_RESTRICTION_FILTER)

        yield get_stats_data(
            sklik_api, account, "queries", start_date, end_date, fields, "total", restriction_filter
        )

    return [
        accounts | groups_stats,
        accounts | banners_stats,
        accounts | ads_stats,
        accounts | queries_stats,
    ]
