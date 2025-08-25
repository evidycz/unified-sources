from typing import List, Iterator

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem
from dlt.extract import DltResource

from .helpers import get_ads_client, get_label_resource_name, get_start_date, to_dict, stats_to_flat_dict


@dlt.source(name="google_ads")
def unified_google_ads_source(
        credentials: dlt.sources.credentials.GcpServiceAccountCredentials = dlt.secrets.value,
        developer_token: str = dlt.secrets.value,
        mcc_customer_id: str = dlt.secrets.value,
        label_name: str | None = None,
) -> List[DltResource]:
    client = get_ads_client(
        credentials=credentials,
        developer_token=developer_token,
        login_customer_id=mcc_customer_id,
    )

    @dlt.resource(
        name="customers",
        table_name="customers",
        write_disposition="skip",

    )
    def customers() -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")

        where_clause = "customer_client.level = 1"
        if label_name:
            label_rn = get_label_resource_name(client, label_name)
            where_clause += f" AND customer_client.applied_labels CONTAINS ANY ('{label_rn}')"

        query = f"""
                SELECT customer_client.id
                FROM customer_client
                WHERE {where_clause}
                """

        stream = gads_service.search_stream(customer_id=client.login_customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.customer_client)

    @dlt.transformer(
        data_from=customers,
        name="accounts",
        table_name="accounts",
        write_disposition="replace",
    )
    def accounts(customer: TDataItem) -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")
        query = """
                SELECT customer.auto_tagging_enabled,
                       customer.call_reporting_setting.call_reporting_enabled,
                       customer.currency_code,
                       customer.descriptive_name,
                       customer.final_url_suffix,
                       customer.id,
                       customer.optimization_score,
                       customer.optimization_score_weight,
                       customer.status,
                       customer.time_zone,
                       customer.tracking_url_template,
                       customer.video_brand_safety_suitability
                FROM customer
                """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.customer)

    @dlt.transformer(
        data_from=customers,
        name="campaigns",
        table_name="campaigns",
        write_disposition="replace",
    )
    def campaigns(customer: TDataItem) -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")
        query = f"""
                SELECT campaign.ad_serving_optimization_status,
                       campaign.advertising_channel_sub_type,
                       campaign.advertising_channel_type,
                       campaign.ai_max_setting.enable_ai_max,
                       campaign.asset_automation_settings,
                       campaign.base_campaign,
                       campaign.bidding_strategy_system_status,
                       campaign.bidding_strategy_type,
                       campaign.brand_guidelines.accent_color,
                       campaign.brand_guidelines.main_color,
                       campaign.brand_guidelines.predefined_font_family,
                       campaign.brand_guidelines_enabled,
                       campaign.experiment_type,
                       campaign.final_url_suffix,
                       campaign.frequency_caps,
                       campaign.geo_target_type_setting.negative_geo_target_type,
                       campaign.geo_target_type_setting.positive_geo_target_type,
                       campaign.id,
                       campaign.keyword_match_type,
                       campaign.maximize_conversion_value.target_roas,
                       campaign.maximize_conversions.target_cpa_micros,
                       campaign.name,
                       campaign.network_settings.target_content_network,
                       campaign.network_settings.target_google_search,
                       campaign.network_settings.target_google_tv_network,
                       campaign.network_settings.target_partner_search_network,
                       campaign.network_settings.target_search_network,
                       campaign.network_settings.target_youtube,
                       campaign.optimization_goal_setting.optimization_goal_types,
                       campaign.optimization_score,
                       campaign.payment_mode,
                       campaign.primary_status,
                       campaign.primary_status_reasons,
                       campaign.serving_status,
                       campaign.status,
                       campaign.tracking_url_template,
                       campaign.url_custom_parameters,
                       campaign.url_expansion_opt_out,
                       customer.id,
                       customer.descriptive_name
                FROM campaign
        """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.campaign)

    @dlt.transformer(
        data_from=customers,
        name="ad_groups",
        table_name="ad_groups",
        write_disposition="replace",
    )
    def ad_groups(customer: TDataItem) -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")
        query = f"""
                SELECT ad_group.ad_rotation_mode,
                       ad_group.ai_max_ad_group_setting.disable_search_term_matching,
                       ad_group.campaign,
                       ad_group.cpc_bid_micros,
                       ad_group.cpm_bid_micros,
                       ad_group.cpv_bid_micros,
                       ad_group.effective_cpc_bid_micros,
                       ad_group.effective_target_cpa_micros,
                       ad_group.effective_target_cpa_source,
                       ad_group.effective_target_roas,
                       ad_group.effective_target_roas_source,
                       ad_group.exclude_demographic_expansion,
                       ad_group.final_url_suffix,
                       ad_group.id,
                       ad_group.name,
                       ad_group.optimized_targeting_enabled,
                       ad_group.primary_status,
                       ad_group.primary_status_reasons,
                       ad_group.status,
                       ad_group.type,
                       campaign.id,
                       campaign.name,
                       customer.id,
                       customer.descriptive_name
                FROM ad_group
        """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.ad_group)

    @dlt.transformer(
        data_from=customers,
        name="asset_groups",
        table_name="asset_groups",
        write_disposition="replace",
    )
    def asset_groups(customer: TDataItem) -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")
        query = f"""
                SELECT asset_group.ad_strength,
                        asset_group.asset_coverage.ad_strength_action_items,
                        asset_group.campaign,
                        asset_group.id,
                        asset_group.name,
                        asset_group.primary_status,
                        asset_group.primary_status_reasons,
                        asset_group.status,
                        campaign.id,
                        campaign.name,
                        customer.id,
                        customer.descriptive_name
                FROM asset_group
        """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.asset_group)

    return [
        customers | accounts,
        customers | campaigns,
        customers | ad_groups,
        customers | asset_groups,
    ]


@dlt.source(name="google_ads")
def unified_google_ads_stats_source(
        credentials: dlt.sources.credentials.GcpServiceAccountCredentials = dlt.secrets.value,
        developer_token: str = dlt.secrets.value,
        mcc_customer_id: str = dlt.secrets.value,
        initial_load_past_days: int = 28,
        attribution_window_days_lag: int = 7,
        search_terms_refresh_days: int = 28,
        label_name: str | None = None,
) -> List[DltResource]:
    client = get_ads_client(
        credentials=credentials,
        developer_token=developer_token,
        login_customer_id=mcc_customer_id,
    )

    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(
        name="customer",
        table_name="customer",
        write_disposition="skip",
    )
    def customers() -> Iterator[TDataItem]:
        gads_service = client.get_service("GoogleAdsService")

        where_clause = "customer_client.level = 1"
        if label_name:
            label_rn = get_label_resource_name(client, label_name)
            where_clause += f" AND customer_client.applied_labels CONTAINS ANY ('{label_rn}')"

        query = f"""
                SELECT customer_client.id
                FROM customer_client
                WHERE {where_clause}
                """

        stream = gads_service.search_stream(customer_id=client.login_customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                yield to_dict(row.customer_client)

    @dlt.transformer()
    def campaigns_stats(customer: TDataItem):
        # TODO Add campaign stats
        pass

    @dlt.transformer()
    def ad_campaigns_conversion_stats(customer: TDataItem):
        # TODO Add ad campaigns conversion stats
        pass

    @dlt.transformer()
    def ad_campaigns_auction_stats(customer: TDataItem):
        # TODO Add ad campaigns auction stats
        pass

    @dlt.transformer(
        data_from=customers,
        name="ad_groups_stats",
        table_name="ad_groups_stats",
        write_disposition="merge",
        merge_key=("date", "id"),
    )
    def ad_groups_stats(
            customer: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            )
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        gads_service = client.get_service("GoogleAdsService")
        query = f"""
                SELECT 
                    ad_group.id,
                    ad_group.campaign,
                    segments.date,
                    segments.device,
                    metrics.all_conversions,
                    metrics.all_conversions_by_conversion_date,
                    metrics.all_conversions_value,
                    metrics.all_conversions_value_by_conversion_date,
                    metrics.clicks,
                    metrics.content_impression_share,
                    metrics.content_rank_lost_impression_share,
                    metrics.conversions,
                    metrics.conversions_by_conversion_date,
                    metrics.conversions_value,
                    metrics.conversions_value_by_conversion_date,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.interactions,
                    metrics.revenue_micros,
                    metrics.search_absolute_top_impression_share,
                    metrics.search_exact_match_impression_share,
                    metrics.search_impression_share,
                    metrics.search_rank_lost_absolute_top_impression_share,
                    metrics.search_rank_lost_impression_share,
                    metrics.search_rank_lost_top_impression_share,
                    metrics.search_top_impression_share,
                    metrics.top_impression_percentage,
                    metrics.view_through_conversions
                FROM ad_group
                WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                    AND metrics.impressions > 0
                """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield stats_to_flat_dict(row)

    @dlt.transformer(
        data_from=customers,
        name="asset_groups_stats",
        table_name="asset_groups_stats",
        write_disposition="merge",
        merge_key=("date", "id"),
    )
    def asset_groups_stats(
            customer: TDataItem,
            refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "date", initial_value=initial_load_start_date_str
            )
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.yesterday().to_date_string()

        gads_service = client.get_service("GoogleAdsService")
        query = f"""
                SELECT
                    asset_group.id,
                    asset_group.campaign,
                    segments.date,
                    segments.device,
                    metrics.all_conversions,
                    metrics.all_conversions_by_conversion_date,
                    metrics.all_conversions_value,
                    metrics.all_conversions_value_by_conversion_date,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.conversions_by_conversion_date,
                    metrics.conversions_value,
                    metrics.conversions_value_by_conversion_date,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.interactions,
                    metrics.revenue_micros,
                    metrics.view_through_conversions
                FROM asset_group  
                WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                    AND metrics.impressions > 0
                """

        stream = gads_service.search_stream(customer_id=customer["id"], query=query)
        for batch in stream:
            for row in batch.results:
                yield stats_to_flat_dict(row)

    @dlt.transformer()
    def assets_stats(customer: TDataItem):
        # TODO Add asset stats
        pass

    @dlt.transformer()
    def shopping_stats(customer: TDataItem):
        # TODO Add shopping stats
        pass

    @dlt.transformer()
    def search_terms_stats(customer: TDataItem):
        # TODO Add search terms stats
        pass

    return [
        customers | ad_groups_stats,
        customers | asset_groups_stats,
    ]
