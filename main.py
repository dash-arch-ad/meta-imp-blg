import os
import json
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

META_API_VERSION = "v25.0"
GOOGLE_ADS_API_VERSION = "v23"
JST = ZoneInfo("Asia/Tokyo")
DEFAULT_WORKSHEET_NAME = "gitreport"


def main():
    print("=== Start Unified Reach Export ===")
    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    monthly_ranges, daily_since, daily_until = get_target_date_ranges()
    monthly_range_text = ", ".join(
        [f"{r['label']}({r['since']} to {r['until']})" for r in monthly_ranges]
    )
    print(f"Target monthly ranges: {monthly_range_text}")
    print(f"Target daily range: {daily_since} to {daily_until}")

    meta_rows = fetch_meta_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        since=daily_since,
        until=daily_until,
    )
    print(f"Meta rows built: {len(meta_rows)}")

    tiktok_fetch_since = get_tiktok_daily_fetch_since(daily_until)
    tiktok_rows = fetch_tiktok_rows(
        advertiser_id=resolved["tiktok"]["advertiser_id"],
        access_token=resolved["tiktok"]["access_token"],
        monthly_ranges=monthly_ranges,
        output_since=daily_since,
        output_until=daily_until,
        daily_fetch_since=tiktok_fetch_since,
    )
    print(f"TikTok rows built: {len(tiktok_rows)}")

    # Google Ads はまだ未稼働のため停止
    # google_rows = fetch_google_ads_rows(
    #     google_ads_conf=resolved["google_ads"],
    #     monthly_ranges=monthly_ranges,
    #     daily_since=daily_since,
    #     daily_until=daily_until,
    # )
    google_rows = []
    print("Google Ads rows built: 0 (disabled)")

    all_rows = sort_rows(meta_rows + tiktok_rows + google_rows)

    spreadsheet = connect_spreadsheet(
        sheet_id=resolved["sheet"]["spreadsheet_id"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )
    write_to_sheet(
        spreadsheet=spreadsheet,
        sheet_name=resolved["sheet"]["worksheet_name"],
        rows=all_rows,
    )

    print(f"Total rows written: {len(all_rows)}")
    print("=== Completed ===")


def load_secret():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise RuntimeError("APP_SECRET_JSON is not set")

    try:
        return json.loads(secret_env)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"APP_SECRET_JSON is invalid JSON: {e}") from e


def mask_sensitive_values(config):
    candidates = []

    def push(value):
        if value is None:
            return
        value = str(value).strip()
        if not value:
            return
        if "\n" in value:
            return
        candidates.append(value)

    meta = config.get("meta", {})
    tiktok = config.get("tiktok", {})

    push(meta.get("token"))
    push(config.get("m_token"))
    push(meta.get("account_id"))
    push(config.get("m_act_id"))
    push(tiktok.get("access_token"))
    push(tiktok.get("advertiser_id"))

    for value in sorted(set(candidates)):
        print(f"::add-mask::{value}")


def resolve_config(config):
    meta_conf = config.get("meta", {})
    tiktok_conf = config.get("tiktok", {})
    google_ads_conf = config.get("google_ads", {})
    sheets_conf = config.get("sheets", {})

    spreadsheet_id = sheets_conf.get("spreadsheet_id")
    if not spreadsheet_id:
        legacy_sheet_id = config.get("s_id")
        if isinstance(legacy_sheet_id, list):
            spreadsheet_id = legacy_sheet_id[0] if legacy_sheet_id else None
        else:
            spreadsheet_id = legacy_sheet_id

    worksheet_name = sheets_conf.get("worksheet_name") or DEFAULT_WORKSHEET_NAME

    google_service_account = config.get("gcp_service_account") or config.get("g_creds")
    google_service_account = normalize_google_service_account(google_service_account)

    return {
        "meta": {
            "token": meta_conf.get("token") or config.get("m_token"),
            "account_id": meta_conf.get("account_id") or config.get("m_act_id"),
        },
        "tiktok": {
            "access_token": tiktok_conf.get("access_token"),
            "advertiser_id": tiktok_conf.get("advertiser_id"),
        },
        "google_ads": {
            "developer_token": google_ads_conf.get("developer_token"),
            "client_id": google_ads_conf.get("client_id"),
            "client_secret": google_ads_conf.get("client_secret"),
            "refresh_token": google_ads_conf.get("refresh_token"),
            "customer_id": normalize_customer_id(google_ads_conf.get("customer_id")),
            "login_customer_id": normalize_customer_id(
                google_ads_conf.get("login_customer_id")
            ),
        },
        "sheet": {
            "spreadsheet_id": spreadsheet_id,
            "worksheet_name": worksheet_name,
            "google_service_account": google_service_account,
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_id": resolved["meta"]["account_id"],
        "tiktok.access_token": resolved["tiktok"]["access_token"],
        "tiktok.advertiser_id": resolved["tiktok"]["advertiser_id"],
        "sheet.spreadsheet_id": resolved["sheet"]["spreadsheet_id"],
        "sheet.google_service_account": resolved["sheet"]["google_service_account"],
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required config keys: {', '.join(missing)}")


def normalize_google_service_account(creds):
    if not creds:
        return None

    fixed = dict(creds)
    private_key = fixed.get("private_key", "")
    if private_key:
        fixed["private_key"] = private_key.replace("\\n", "\n")
    return fixed


def normalize_customer_id(value):
    if value is None:
        return None
    value = str(value).strip().replace("-", "")
    return value or None


def normalize_meta_act_id(raw_act_id):
    cleaned = (
        str(raw_act_id)
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )
    return f"act_{cleaned}"


def normalize_day_str(value):
    if not value:
        return ""
    value = str(value).strip()
    return value[:10]


def get_target_date_ranges():
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    this_month_start = date(today_jst.year, today_jst.month, 1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = date(last_month_end.year, last_month_end.month, 1)

    monthly_ranges = [
        {
            "label": last_month_start.strftime("%Y-%m"),
            "since": last_month_start,
            "until": last_month_end,
        }
    ]

    if yesterday >= this_month_start:
        monthly_ranges.append(
            {
                "label": this_month_start.strftime("%Y-%m"),
                "since": this_month_start,
                "until": yesterday,
            }
        )

    return monthly_ranges, last_month_start, yesterday


def get_tiktok_daily_fetch_since(until):
    return until - timedelta(days=63)


def make_output_row(
    media,
    scope,
    period,
    campaign_name="",
    adset_name="",
    ad_name="",
    unique_reach=0,
    unique_ad_recall_lift="",
):
    return [
        media,
        scope,
        period,
        campaign_name or "",
        adset_name or "",
        ad_name or "",
        to_int(unique_reach),
        to_int_or_blank(unique_ad_recall_lift),
    ]


def to_int_or_blank(value):
    if value in (None, ""):
        return ""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return ""


def get_nested(data, *keys, default=""):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def fetch_meta_rows(act_id, token, since, until):
    normalized_act_id = normalize_meta_act_id(act_id)
    rows = []

    common_fields = ["reach", "estimated_ad_recallers"]

    account_monthly = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="monthly",
        level="account",
        fields=common_fields,
    )
    for item in account_monthly:
        rows.append(
            make_output_row(
                media="meta",
                scope="all",
                period=to_month(item.get("date_start")),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    account_daily = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="1",
        level="account",
        fields=common_fields,
    )
    for item in account_daily:
        rows.append(
            make_output_row(
                media="meta",
                scope="day",
                period=item.get("date_start", ""),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    campaign_daily = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="1",
        level="campaign",
        fields=["campaign_name", "reach", "estimated_ad_recallers"],
    )
    for item in campaign_daily:
        rows.append(
            make_output_row(
                media="meta",
                scope="campaign_day",
                period=item.get("date_start", ""),
                campaign_name=item.get("campaign_name", ""),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    campaign_monthly = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="monthly",
        level="campaign",
        fields=["campaign_name", "reach", "estimated_ad_recallers"],
    )
    for item in campaign_monthly:
        rows.append(
            make_output_row(
                media="meta",
                scope="campaign",
                period=to_month(item.get("date_start")),
                campaign_name=item.get("campaign_name", ""),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    adset_monthly = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="monthly",
        level="adset",
        fields=["campaign_name", "adset_name", "reach", "estimated_ad_recallers"],
    )
    for item in adset_monthly:
        rows.append(
            make_output_row(
                media="meta",
                scope="adset",
                period=to_month(item.get("date_start")),
                campaign_name=item.get("campaign_name", ""),
                adset_name=item.get("adset_name", ""),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    ad_monthly = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        time_increment="monthly",
        level="ad",
        fields=[
            "campaign_name",
            "adset_name",
            "ad_name",
            "reach",
            "estimated_ad_recallers",
        ],
    )
    for item in ad_monthly:
        rows.append(
            make_output_row(
                media="meta",
                scope="ad",
                period=to_month(item.get("date_start")),
                campaign_name=item.get("campaign_name", ""),
                adset_name=item.get("adset_name", ""),
                ad_name=item.get("ad_name", ""),
                unique_reach=item.get("reach"),
                unique_ad_recall_lift=item.get("estimated_ad_recallers"),
            )
        )

    return rows


def fetch_meta_insights(act_id, token, since, until, time_increment, level, fields):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"
    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps(
            {
                "since": since.strftime("%Y-%m-%d"),
                "until": until.strftime("%Y-%m-%d"),
            }
        ),
        "fields": ",".join(fields),
        "time_increment": time_increment,
        "limit": 5000,
    }

    all_rows = []

    while True:
        response = requests.get(url, params=params, timeout=120)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"Meta API request failed. status={response.status_code}, body={truncate_text(response.text)}"
            ) from e

        data = response.json()
        if "error" in data:
            raise RuntimeError(
                f"Meta API error: {json.dumps(data['error'], ensure_ascii=False)}"
            )

        batch = data.get("data", [])
        all_rows.extend(batch)

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


def fetch_tiktok_rows(
    advertiser_id,
    access_token,
    monthly_ranges,
    output_since,
    output_until,
    daily_fetch_since,
):
    rows = []

    def safe_fetch(scope_name, **kwargs):
        try:
            return fetch_tiktok_report(**kwargs)
        except RuntimeError as e:
            print(f"Warning: TikTok {scope_name} skipped: {e}")
            return []

    # all（月次） - 失敗しても全体停止しない
    for month_range in monthly_ranges:
        batch = safe_fetch(
            "all",
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_ADVERTISER",
            dimensions=["advertiser_id"],
            metrics=["reach"],
            since=month_range["since"],
            until=month_range["until"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="all",
                    period=month_range["label"],
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    # day（日次） - 失敗しても全体停止しない
    for chunk_since, chunk_until in split_date_ranges(daily_fetch_since, output_until, 30):
        batch = safe_fetch(
            "day",
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_ADVERTISER",
            dimensions=["stat_time_day"],
            metrics=["reach"],
            since=chunk_since,
            until=chunk_until,
        )
        for item in batch:
            day = normalize_day_str(extract_tiktok_dimension(item, "stat_time_day"))
            if not day:
                continue
            if not is_in_date_range(day, output_since, output_until):
                continue

            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="day",
                    period=day,
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    # campaign_day（日次） - 失敗しても全体停止しない
    for chunk_since, chunk_until in split_date_ranges(daily_fetch_since, output_until, 30):
        batch = safe_fetch(
            "campaign_day",
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_CAMPAIGN",
            dimensions=["campaign_id", "stat_time_day"],
            metrics=["campaign_name", "reach"],
            since=chunk_since,
            until=chunk_until,
        )
        for item in batch:
            day = normalize_day_str(extract_tiktok_dimension(item, "stat_time_day"))
            if not day:
                continue
            if not is_in_date_range(day, output_since, output_until):
                continue

            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="campaign_day",
                    period=day,
                    campaign_name=extract_tiktok_metric(item, "campaign_name"),
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    # campaign（月次） - 失敗しても全体停止しない
    for month_range in monthly_ranges:
        batch = safe_fetch(
            "campaign",
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_CAMPAIGN",
            dimensions=["campaign_id"],
            metrics=["campaign_name", "reach"],
            since=month_range["since"],
            until=month_range["until"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="campaign",
                    period=month_range["label"],
                    campaign_name=extract_tiktok_metric(item, "campaign_name"),
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    # adset（月次） - 失敗しても全体停止しない
    for month_range in monthly_ranges:
        batch = safe_fetch(
            "adset",
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_ADGROUP",
            dimensions=["adgroup_id"],
            metrics=["campaign_name", "adgroup_name", "reach"],
            since=month_range["since"],
            until=month_range["until"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="adset",
                    period=month_range["label"],
                    campaign_name=extract_tiktok_metric(item, "campaign_name"),
                    adset_name=extract_tiktok_metric(item, "adgroup_name"),
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    # ad（月次） - 元コードで実績のある軸
    for month_range in monthly_ranges:
        batch = fetch_tiktok_report(
            advertiser_id=advertiser_id,
            access_token=access_token,
            data_level="AUCTION_AD",
            dimensions=["ad_id"],
            metrics=["campaign_name", "adgroup_name", "ad_name", "reach"],
            since=month_range["since"],
            until=month_range["until"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="tiktok",
                    scope="ad",
                    period=month_range["label"],
                    campaign_name=extract_tiktok_metric(item, "campaign_name"),
                    adset_name=extract_tiktok_metric(item, "adgroup_name"),
                    ad_name=extract_tiktok_metric(item, "ad_name"),
                    unique_reach=extract_tiktok_metric(item, "reach"),
                    unique_ad_recall_lift="",
                )
            )

    return rows


def fetch_tiktok_report(
    advertiser_id,
    access_token,
    data_level,
    dimensions,
    metrics,
    since,
    until,
):
    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    headers = {
        "Access-Token": access_token,
    }

    page = 1
    page_size = 1000
    all_rows = []

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "service_type": "AUCTION",
            "data_level": data_level,
            "dimensions": json.dumps(dimensions, separators=(",", ":")),
            "metrics": json.dumps(metrics, separators=(",", ":")),
            "start_date": since.strftime("%Y-%m-%d"),
            "end_date": until.strftime("%Y-%m-%d"),
            "page": page,
            "page_size": page_size,
            "query_lifetime": "false",
            "enable_total_metrics": "false",
            "multi_adv_report_in_utc_time": "false",
        }

        response = requests.get(url, headers=headers, params=params, timeout=120)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"TikTok report API request failed. status={response.status_code}, body={truncate_text(response.text)}"
            ) from e

        payload = response.json()
        code = payload.get("code")
        if code not in (0, "0"):
            raise RuntimeError(
                f"TikTok report API error: code={code}, message={payload.get('message')}, request_id={payload.get('request_id')}"
            )

        data = payload.get("data", {})
        batch = data.get("list", [])
        all_rows.extend(batch)

        page_info = data.get("page_info", {})
        total_page = to_int(page_info.get("total_page"))
        total_number = to_int(page_info.get("total_number"))

        if total_page and page >= total_page:
            break
        if total_number and page * page_size >= total_number:
            break
        if len(batch) < page_size:
            break

        page += 1

    return all_rows


def fetch_tiktok_campaign_name_map(advertiser_id, access_token, campaign_ids):
    if not campaign_ids:
        return {}

    url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
    headers = {
        "Access-Token": access_token,
    }

    name_map = {}

    for campaign_id_batch in chunked(campaign_ids, 100):
        params = {
            "advertiser_id": advertiser_id,
            "filtering": json.dumps(
                {"campaign_ids": campaign_id_batch}, separators=(",", ":")
            ),
            "page": 1,
            "page_size": 1000,
        }

        response = requests.get(url, headers=headers, params=params, timeout=120)
        if not response.ok:
            print(
                f"Warning: TikTok campaign/get failed. status={response.status_code}. Fallback to campaign_id."
            )
            continue

        payload = response.json()
        code = payload.get("code")
        if code not in (0, "0"):
            print(
                f"Warning: TikTok campaign/get returned code={code}. Fallback to campaign_id."
            )
            continue

        data = payload.get("data", {})
        for item in data.get("list", []):
            cid = str(item.get("campaign_id", "")).strip()
            cname = item.get("campaign_name", "") or ""
            if cid:
                name_map[cid] = cname or cid

    return name_map


def fetch_tiktok_adgroup_name_map(advertiser_id, access_token, adgroup_ids):
    if not adgroup_ids:
        return {}

    url = "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/"
    headers = {
        "Access-Token": access_token,
    }

    name_map = {}

    for adgroup_id_batch in chunked(adgroup_ids, 100):
        params = {
            "advertiser_id": advertiser_id,
            "filtering": json.dumps(
                {"adgroup_ids": adgroup_id_batch}, separators=(",", ":")
            ),
            "page": 1,
            "page_size": 1000,
        }

        response = requests.get(url, headers=headers, params=params, timeout=120)
        if not response.ok:
            print(
                f"Warning: TikTok adgroup/get failed. status={response.status_code}. Fallback to adgroup_id."
            )
            continue

        payload = response.json()
        code = payload.get("code")
        if code not in (0, "0"):
            print(
                f"Warning: TikTok adgroup/get returned code={code}. Fallback to adgroup_id."
            )
            continue

        data = payload.get("data", {})
        for item in data.get("list", []):
            aid = str(item.get("adgroup_id", "")).strip()
            aname = item.get("adgroup_name", "") or ""
            if aid:
                name_map[aid] = aname or aid

    return name_map


def fetch_tiktok_ad_name_map(advertiser_id, access_token, ad_ids):
    if not ad_ids:
        return {}

    url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
    headers = {
        "Access-Token": access_token,
    }

    name_map = {}

    for ad_id_batch in chunked(ad_ids, 100):
        params = {
            "advertiser_id": advertiser_id,
            "filtering": json.dumps(
                {"ad_ids": ad_id_batch}, separators=(",", ":")
            ),
            "page": 1,
            "page_size": 1000,
        }

        response = requests.get(url, headers=headers, params=params, timeout=120)
        if not response.ok:
            print(
                f"Warning: TikTok ad/get failed. status={response.status_code}. Fallback to ad_id."
            )
            continue

        payload = response.json()
        code = payload.get("code")
        if code not in (0, "0"):
            print(
                f"Warning: TikTok ad/get returned code={code}. Fallback to ad_id."
            )
            continue

        data = payload.get("data", {})
        for item in data.get("list", []):
            aid = str(item.get("ad_id", "")).strip()
            aname = item.get("ad_name", "") or ""
            if aid:
                name_map[aid] = aname or aid

    return name_map


def extract_tiktok_dimension(item, key):
    dimensions = item.get("dimensions", {})
    if isinstance(dimensions, dict) and key in dimensions:
        return dimensions.get(key)

    if key in item:
        return item.get(key)

    return ""


def extract_tiktok_metric(item, key):
    metrics = item.get("metrics", {})
    if isinstance(metrics, dict) and key in metrics:
        return metrics.get(key)

    if isinstance(metrics, list) and metrics:
        for metric_item in metrics:
            if isinstance(metric_item, dict) and key in metric_item:
                return metric_item.get(key)

    if key in item:
        return item.get(key)

    return "" if key in {"campaign_name", "adgroup_name", "ad_name"} else 0


def fetch_google_ads_rows(google_ads_conf, monthly_ranges, daily_since, daily_until):
    access_token = refresh_google_ads_access_token(
        client_id=google_ads_conf["client_id"],
        client_secret=google_ads_conf["client_secret"],
        refresh_token=google_ads_conf["refresh_token"],
    )

    rows = []

    for month_range in monthly_ranges:
        query = f"""
            SELECT
              metrics.unique_users
            FROM customer
            WHERE segments.date BETWEEN '{month_range["since"]:%Y-%m-%d}' AND '{month_range["until"]:%Y-%m-%d}'
        """.strip()

        result_rows = google_ads_search_stream(
            access_token=access_token,
            developer_token=google_ads_conf["developer_token"],
            customer_id=google_ads_conf["customer_id"],
            login_customer_id=google_ads_conf["login_customer_id"],
            query=query,
        )

        for item in result_rows:
            rows.append(
                make_output_row(
                    media="google",
                    scope="all",
                    period=month_range["label"],
                    unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                    unique_ad_recall_lift="",
                )
            )

    daily_all_query = f"""
        SELECT
          segments.date,
          metrics.unique_users
        FROM customer
        WHERE segments.date BETWEEN '{daily_since:%Y-%m-%d}' AND '{daily_until:%Y-%m-%d}'
        ORDER BY segments.date
    """.strip()

    daily_all_rows = google_ads_search_stream(
        access_token=access_token,
        developer_token=google_ads_conf["developer_token"],
        customer_id=google_ads_conf["customer_id"],
        login_customer_id=google_ads_conf["login_customer_id"],
        query=daily_all_query,
    )

    for item in daily_all_rows:
        rows.append(
            make_output_row(
                media="google",
                scope="day",
                period=get_nested(item, "segments", "date", default=""),
                unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                unique_ad_recall_lift="",
            )
        )

    daily_campaign_query = f"""
        SELECT
          campaign.id,
          campaign.name,
          segments.date,
          metrics.unique_users
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND segments.date BETWEEN '{daily_since:%Y-%m-%d}' AND '{daily_until:%Y-%m-%d}'
        ORDER BY segments.date, campaign.id
    """.strip()

    daily_campaign_rows = google_ads_search_stream(
        access_token=access_token,
        developer_token=google_ads_conf["developer_token"],
        customer_id=google_ads_conf["customer_id"],
        login_customer_id=google_ads_conf["login_customer_id"],
        query=daily_campaign_query,
    )

    for item in daily_campaign_rows:
        rows.append(
            make_output_row(
                media="google",
                scope="campaign_day",
                period=get_nested(item, "segments", "date", default=""),
                campaign_name=get_nested(item, "campaign", "name", default=""),
                unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                unique_ad_recall_lift="",
            )
        )

    for month_range in monthly_ranges:
        query = f"""
            SELECT
              campaign.id,
              campaign.name,
              metrics.unique_users
            FROM campaign
            WHERE campaign.status != 'REMOVED'
              AND segments.date BETWEEN '{month_range["since"]:%Y-%m-%d}' AND '{month_range["until"]:%Y-%m-%d}'
            ORDER BY campaign.id
        """.strip()

        result_rows = google_ads_search_stream(
            access_token=access_token,
            developer_token=google_ads_conf["developer_token"],
            customer_id=google_ads_conf["customer_id"],
            login_customer_id=google_ads_conf["login_customer_id"],
            query=query,
        )

        for item in result_rows:
            rows.append(
                make_output_row(
                    media="google",
                    scope="campaign",
                    period=month_range["label"],
                    campaign_name=get_nested(item, "campaign", "name", default=""),
                    unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                    unique_ad_recall_lift="",
                )
            )

    for month_range in monthly_ranges:
        query = f"""
            SELECT
              campaign.name,
              ad_group.id,
              ad_group.name,
              metrics.unique_users
            FROM ad_group
            WHERE campaign.status != 'REMOVED'
              AND ad_group.status != 'REMOVED'
              AND segments.date BETWEEN '{month_range["since"]:%Y-%m-%d}' AND '{month_range["until"]:%Y-%m-%d}'
            ORDER BY campaign.name, ad_group.id
        """.strip()

        result_rows = google_ads_search_stream(
            access_token=access_token,
            developer_token=google_ads_conf["developer_token"],
            customer_id=google_ads_conf["customer_id"],
            login_customer_id=google_ads_conf["login_customer_id"],
            query=query,
        )

        for item in result_rows:
            rows.append(
                make_output_row(
                    media="google",
                    scope="adset",
                    period=month_range["label"],
                    campaign_name=get_nested(item, "campaign", "name", default=""),
                    adset_name=get_nested(item, "adGroup", "name", default=""),
                    unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                    unique_ad_recall_lift="",
                )
            )

    for month_range in monthly_ranges:
        query = f"""
            SELECT
              campaign.name,
              ad_group.name,
              ad_group_ad.ad.id,
              ad_group_ad.ad.name,
              metrics.unique_users
            FROM ad_group_ad
            WHERE campaign.status != 'REMOVED'
              AND ad_group.status != 'REMOVED'
              AND ad_group_ad.status != 'REMOVED'
              AND segments.date BETWEEN '{month_range["since"]:%Y-%m-%d}' AND '{month_range["until"]:%Y-%m-%d}'
            ORDER BY campaign.name, ad_group.name, ad_group_ad.ad.id
        """.strip()

        result_rows = google_ads_search_stream(
            access_token=access_token,
            developer_token=google_ads_conf["developer_token"],
            customer_id=google_ads_conf["customer_id"],
            login_customer_id=google_ads_conf["login_customer_id"],
            query=query,
        )

        for item in result_rows:
            ad_name = get_nested(item, "adGroupAd", "ad", "name", default="")
            if not ad_name:
                ad_id = get_nested(item, "adGroupAd", "ad", "id", default="")
                ad_name = str(ad_id) if ad_id != "" else ""

            rows.append(
                make_output_row(
                    media="google",
                    scope="ad",
                    period=month_range["label"],
                    campaign_name=get_nested(item, "campaign", "name", default=""),
                    adset_name=get_nested(item, "adGroup", "name", default=""),
                    ad_name=ad_name,
                    unique_reach=get_nested(item, "metrics", "uniqueUsers", default=0),
                    unique_ad_recall_lift="",
                )
            )

    return rows


def refresh_google_ads_access_token(client_id, client_secret, refresh_token):
    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=120,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Google OAuth token refresh failed. status={response.status_code}, body={truncate_text(response.text)}"
        ) from e

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Google OAuth token refresh returned no access_token: {payload}")

    print("Google Ads OAuth token refreshed successfully")
    return access_token


def google_ads_search_stream(
    access_token, developer_token, customer_id, login_customer_id, query
):
    url = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    if login_customer_id:
        headers["login-customer-id"] = login_customer_id

    response = requests.post(url, headers=headers, json={"query": query}, timeout=120)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Google Ads API request failed. status={response.status_code}, body={truncate_text(response.text)}"
        ) from e

    payload = response.json()

    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(
            f"Google Ads API error: {truncate_text(json.dumps(payload['error'], ensure_ascii=False))}"
        )

    if not isinstance(payload, list):
        raise RuntimeError(
            f"Google Ads API unexpected response shape: {truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )

    all_rows = []
    for chunk in payload:
        all_rows.extend(chunk.get("results", []))

    return all_rows


def connect_spreadsheet(sheet_id, google_creds_dict):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            google_creds_dict, scope
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        print("Google Sheets connected successfully")
        return spreadsheet
    except Exception as e:
        raise RuntimeError(f"Google Sheets connection error: {repr(e)}") from e


def write_to_sheet(spreadsheet, sheet_name, rows):
    header = [[
        "media",
        "scope",
        "period",
        "campaign_name",
        "adset_name",
        "ad_name",
        "unique_reach",
        "unique_ad_recall_lift",
    ]]

    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=8)

        worksheet.clear()
        output = header + rows
        worksheet.update("A1", output)
        print(f"Write success: {sheet_name} ({len(rows)} rows)")
    except Exception as e:
        raise RuntimeError(f"Write error ({sheet_name}): {repr(e)}") from e


def sort_rows(rows):
    media_order = {"meta": 0, "tiktok": 1, "google": 2}
    scope_order = {
        "all": 0,
        "day": 1,
        "campaign_day": 2,
        "campaign": 3,
        "adset": 4,
        "ad": 5,
    }

    def sort_key(row):
        media, scope, period, campaign_name, adset_name, ad_name, _reach, _lift = row
        period_num = int(str(period).replace("-", "")) if period else 0
        return (
            media_order.get(media, 999),
            -period_num,
            scope_order.get(scope, 999),
            campaign_name,
            adset_name,
            ad_name,
        )

    return sorted(rows, key=sort_key)


def split_date_ranges(since, until, max_days):
    ranges = []
    current_since = since

    while current_since <= until:
        current_until = min(current_since + timedelta(days=max_days - 1), until)
        ranges.append((current_since, current_until))
        current_since = current_until + timedelta(days=1)

    return ranges


def is_in_date_range(day_str, since, until):
    normalized = normalize_day_str(day_str)
    if not normalized:
        return False
    target = datetime.strptime(normalized, "%Y-%m-%d").date()
    return since <= target <= until


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def to_month(value):
    if not value:
        return ""
    return str(value)[:7]


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def truncate_text(value, limit=800):
    value = str(value)
    if len(value) <= limit:
        return value
    return value[:limit] + "...(truncated)"


if __name__ == "__main__":
    main()
