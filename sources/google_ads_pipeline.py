import dlt

from google_ads import unified_google_ads_source


def load_settings() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_settings",
        destination="duckdb",
        dataset_name="google_ads",
    )
    setting_source = unified_google_ads_source()
    load_info = pipeline.run(setting_source)
    print(load_info)


if __name__ == "__main__":
    load_settings()
