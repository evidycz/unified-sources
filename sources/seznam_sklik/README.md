---
title: Seznam Sklik
description: dlt source for Seznam Sklik API
keywords: [seznam sklik api, seznam sklik source, sklik]
---


# Seznam Sklik

[Seznam Sklik](https://www.sklik.cz/) is an online advertising platform by Seznam.cz that allows advertisers to create and manage PPC (Pay-Per-Click) campaigns for search and display advertising in the Czech Republic.

Resources that can be loaded using this verified source are:

| Name | Description |
| ---- | ----------- |
| accounts | Retrieves account information including user_id, username, wallet details |
| campaigns | Retrieves campaign settings including name, status, budget, dates |
| groups | Retrieves group settings including name, status, campaign details, pricing |
| groups_stats | Retrieves group statistics including impressions, clicks, conversions by device type |
| ads_stats | Retrieves ad statistics and content including headlines, descriptions, performance metrics |
| banners_stats | Retrieves banner ad statistics including image details, performance metrics |
| queries_stats | Retrieves query statistics including keyword details, performance metrics |
| retargeting_stats | Retrieves retargeting statistics including users, impressions, conversions |

## Initialize the pipeline

```bash
dlt init seznam_sklik duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Add credentials

1. [Seznam Sklik API](https://api.sklik.cz/drak/) requires authentication. You need to obtain an access token from Seznam Sklik and add it to your `secrets.toml` file:

   ```toml
   [sources.seznam_sklik]
   access_token = "your_access_token_here"
   ```

2. Follow the instructions in the [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) document to add credentials for your chosen destination.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Now the pipeline can be run by using the command:

   ```bash
   python seznam_sklik_pipeline.py
   ```

   The pipeline provides two main functions:
   - `load_settings()`: Loads account, campaign, and group settings
   - `load_stats()`: Loads statistical data including groups_stats, ads_stats, banners_stats, and queries_stats

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline seznam_sklik_settings show
   dlt pipeline seznam_sklik_stats show
   ```

## Configuration Options

The Seznam Sklik source provides several configuration options:

- `initial_load_past_days`: Number of days to load data for on initial load (default: 28)
- `attribution_window_days_lag`: Number of days to lag for attribution window (default: 7)
- `queries_refresh_days`: Number of days to refresh queries data (default: 28)
