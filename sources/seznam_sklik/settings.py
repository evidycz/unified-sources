"""Seznam Sklik source settings and constants"""

SKLIK_DATA_FORMAT = "YYYYMMDD"

OBJECT_CONNECTION = (
    "campaign.id",
    "campaign.name",
    "group.id",
    "group.name",
)

DEFAULT_SETTINGS_FIELDS = (
    "id",
    "name",
    "status",
)

DEFAULT_CAMPAIGN_SETTINGS_FIELDS = DEFAULT_SETTINGS_FIELDS + (
    "type",
    "deleted",
    "budget.id",
    "budget.name",
    "budget.colorCodeId",
    "budget.dayBudget",
    "contextNetwork",
    "createDate",
    "deleteDate",
    "startDate",
    "endDate",
    "adSelection",
    "devicesPriceRatio",
    "exhaustedTotalBudget",
    "totalClicks",
    "totalClicksFrom",
    "totalBudget",
    "totalBudgetFrom",
    "videoFormat",
)

DEFAULT_GROUP_SETTINGS_FIELDS = DEFAULT_SETTINGS_FIELDS + (
    "campaign.id",
    "campaign.name",
    "createDate",
    "deleteDate",
    "maxCpc",
    "maxCpt",
    "devicesPriceRatio",
)

DEFAULT_AD_SETTINGS_FIELDS = DEFAULT_SETTINGS_FIELDS + OBJECT_CONNECTION + (
    "adStatus",
    "adType",
    "finalUrl",
    "createDate",
    "deleteDate",
    "headline1",
    "headline2",
    "headline3",
    "longLine",
    "shortLine",
    "description",
    "description2",
)

DEFAULT_BANNER_SETTINGS_FIELDS = OBJECT_CONNECTION + (
    "id",
    "bannerName",
    "status",
    "adStatus",
    "adType",
    "createDate",
    "deleteDate",
    "bannerName",
    "image.width",
    "image.height",
)

DEFAULT_SITELINKS_SETTINGS_FIELDS = OBJECT_CONNECTION + DEFAULT_SETTINGS_FIELDS

STATS_FIELDS = (
    "id",
    "name",
    "impressions",
    "clicks",
    "totalMoney",
)

CONVERSIONS_FIELDS = (
    "avgPos",
    "conversions",
    "conversionValue",
)

IMPRESSION_SHARE_FIELDS = (
    "missImpressions",
    "underLowerThreshold",
    "exhaustedBudget",
    "stoppedBySchedule",
    "underForestThreshold",
    "exhaustedBudgetShare",
    "ish",
    "ishContext",
    "ishSum",
)

DEFAULT_STATS_FIELDS = STATS_FIELDS + CONVERSIONS_FIELDS + IMPRESSION_SHARE_FIELDS

DEFAULT_CAMPAIGN_STATS_FIELDS = DEFAULT_STATS_FIELDS

DEFAULT_GROUP_STATS_FIELDS = DEFAULT_STATS_FIELDS + (
    "campaign.id",
    "campaign.name",
    "winRate",
)

DEFAULT_AD_STATS_FIELDS = OBJECT_CONNECTION + DEFAULT_STATS_FIELDS + (
    "views",
    "viewershipRate",
    "skipRate",
)

DEFAULT_BANNER_STATS_FIELDS = OBJECT_CONNECTION + (
    "id",
    "bannerName",
    "imageType",
    "impressions",
    "clicks",
    "conversions",
    "conversionValue",
    "totalMoney",
    "avgPos",
    "missImpressions",
    "underLowerThreshold",
    "exhaustedBudget",
    "stoppedBySchedule",
    "underForestThreshold",
    "exhaustedBudgetShare",
    "ish",
    "ishContext",
    "ishSum",
)

DEFAULT_SITELINKS_STATS_FIELDS = OBJECT_CONNECTION + STATS_FIELDS

DEFAULT_RETARGETING_STATS_FIELDS = OBJECT_CONNECTION + DEFAULT_STATS_FIELDS + (
    "retargetingId",
    "users",
)

DEFAULT_QUERIES_STATS_FIELDS = OBJECT_CONNECTION + (
    "query",
    "keyword.id",
    "keyword.name",
    "keyword.status",
    "keyword.matchType",
    "keyword.maxCpc",
    "keyword.cpc",
    "keyword.url",
    "group.maxCpc",
    "avgCpc",
    "avgPos",
    "impressions",
    "clicks",
    "conversions",
    "conversionValue",
    "totalMoney",
)

STATS_PRIMARY_KEY = ["id", "date"]

OBJECT_SETTINGS_COLUMNS = {
    "account_id": {"data_type": "text"},
    "account_name": {"data_type": "text"},
    "id": {"data_type": "text"},
}

OBJECT_STATS_COLUMNS = {
    "date": {"data_type": "date"},
} | OBJECT_SETTINGS_COLUMNS

DEFAULT_RESTRICTION_FILTER = {"statisticsConditions": [{"columnName": "impressions", "operator": "GT", "intValue": 0}]}