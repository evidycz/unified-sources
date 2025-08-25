import json
import tempfile
from typing import Any, Iterator

import dlt
import proto
from dlt.common import pendulum
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient


def get_ads_client(
        credentials: GcpServiceAccountCredentials,
        developer_token: str,
        login_customer_id: str,
) -> GoogleAdsClient:
    with tempfile.NamedTemporaryFile() as f:
        f.write(credentials.to_native_representation().encode())
        f.seek(0)

        return GoogleAdsClient.load_from_dict(
            config_dict={
                "json_key_file_path": f.name,
                "developer_token": developer_token,
                "login_customer_id": login_customer_id,
                "use_proto_plus": True,
            }
        )


def get_label_resource_name(client: GoogleAdsClient, label_name: str) -> str:
    gads_service = client.get_service("GoogleAdsService")
    query = f"""
                SELECT
                    label.resource_name,
                    label.name
                FROM label
                WHERE label.name = '{label_name}'
                LIMIT 1
            """
    stream = gads_service.search_stream(customer_id=client.login_customer_id, query=query)
    for batch in stream:
        for row in batch.results:
            return row.label.resource_name

    raise ValueError(f"Label with name '{label_name}' not found under MCC {client.login_customer_id}.")


def get_start_date(
        incremental_start_date: dlt.sources.incremental[str],
        attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    start_date: pendulum.DateTime = ensure_pendulum_datetime(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

    # lag the incremental start date by attribution window lag
    incremental_start_date.start_value = start_date.isoformat()
    return start_date


def to_dict(item: Any) -> Iterator[TDataItem]:
    yield json.loads(
        proto.Message.to_json(
            item,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
            including_default_value_fields=False,
        )
    )


def stats_to_flat_dict(item: Any) -> Iterator[TDataItem]:
    for row in to_dict(item):
        yield {k: v for d in row.values() if isinstance(d, dict) for k, v in d.items()}