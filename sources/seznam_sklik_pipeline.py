import dlt

from seznam_sklik import unified_sklik_source, unified_sklik_stat_source


def load_settings(access_token: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_settings",
        destination="duckdb",
        dataset_name="seznam_sklik",
    )
    setting_source = unified_sklik_source(access_token)
    load_info = pipeline.run(setting_source)
    print(load_info)

def load_stats(access_token: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_stats",
        destination="duckdb",
        dataset_name="seznam_sklik",
    )
    setting_source = unified_sklik_stat_source(access_token)
    load_info = pipeline.run(setting_source)
    print(load_info)


if __name__ == "__main__":
    access_token = "0xff1206bea62c411ecd0e46beb485b279f464aa9b69d2080a799af0766daba26599e25"

    # load_settings(access_token)
    load_stats(access_token)