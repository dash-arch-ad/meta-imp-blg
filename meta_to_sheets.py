import os
import json
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

API_VERSION = "v25.0"
JST = ZoneInfo("Asia/Tokyo")

REPORT_RULES = {
    "META_RE_MO": {
        "sheet_key": "meta_re_mo",
        "level": "account",
        "time_increment": "monthly",
        "fields": ["reach"],
        "header": ["Month", "Reach"]
    },
    "META_RE_DAY": {
        "sheet_key": "meta_re_day",
        "level": "account",
        "time_increment": "1",
        "fields": ["reach"],
        "header": ["Month", "Day", "Reach"]
    },
    "META_RE_AD": {
        "sheet_key": "meta_re_ad",
        "level": "ad",
        "time_increment": "monthly",
        "fields": ["campaign_name", "adset_name", "ad_name", "reach"],
        "header": ["Month", "Campaign name", "Adset Name", "Ad Name", "Reach"]
    }
}


def main():
    print("=== Start Meta Reach Export ===")

    config = load_secret()
    if not config:
        return

    meta_token = config.get("m_token")
    raw_act_id = str(config.get("m_act_id", "")).strip()
    sheet_id = config.get("s_id")
    sheets_map = config.get("sheets", {})
    google_creds_dict = config.get("g_creds")

    if isinstance(sheet_id, list):
        sheet_id = sheet_id[0] if sheet_id else None

    if not meta_token:
        print("Error: m_token is missing")
        return
    if not raw_act_id:
        print("Error: m_act_id is missing")
        return
    if not sheet_id:
        print("Error: s_id is missing")
        return
    if not google_creds_dict:
        print("Error: g_creds is missing")
        return

    google_creds_dict = normalize_google_creds(google_creds_dict)

    act_id = normalize_act_id(raw_act_id)
    print(f"Target account: {mask_act_id(act_id)}")
    print(f"Spreadsheet ID: {sheet_id[:6]}...")

    spreadsheet = connect_spreadsheet(sheet_id, google_creds_dict)
    if spreadsheet is None:
        return

    since, until = get_target_date_range()
    print(f"Target range: {since} to {until}")

    for rule_key, rule in REPORT_RULES.items():
        sheet_name = sheets_map.get(rule["sheet_key"])
        if not sheet_name:
            print(f"Skip {rule_key}: sheets['{rule['sheet_key']}'] is missing")
            continue

        print(f"\n--- Processing {rule_key} / {sheet_name} ---")
        rows = fetch_and_build_rows(
            act_id=act_id,
            token=meta_token,
            since=since,
            until=until,
            rule=rule
        )
        write_to_sheet(spreadsheet, sheet_name, rule["header"], rows)

    print("\n=== Completed ===")


def load_secret():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        print("Error: APP_SECRET_JSON is not set")
        return None

    try:
        config = json.loads(secret_env)
        print("APP_SECRET_JSON loaded successfully")
        return config
    except json.JSONDecodeError as e:
        print(f"Error: APP_SECRET_JSON is invalid JSON: {e}")
        return None


def normalize_google_creds(creds):
    fixed = dict(creds)

    private_key = fixed.get("private_key", "")
    if private_key:
        fixed["private_key"] = private_key.replace("\\n", "\n")

    return fixed


def normalize_act_id(raw_act_id):
    cleaned = (
        raw_act_id
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )
    return f"act_{cleaned}"


def mask_act_id(act_id):
    num = act_id.replace("act_", "")
    return f"******{num[-4:]}" if len(num) > 4 else act_id


def connect_spreadsheet(sheet_id, google_creds_dict):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        print("Google Sheets connected successfully")
        return spreadsheet
    except Exception as e:
        print(f"Google Sheets connection error: {repr(e)}")
        return None


def get_target_date_range():
    now_jst = datetime.now(JST)
    today_jst = now_jst.date()
    yesterday = today_jst - timedelta(days=1)

    this_month_start = date(today_jst.year, today_jst.month, 1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = date(last_month_end.year, last_month_end.month, 1)

    return last_month_start, yesterday


def fetch_and_build_rows(act_id, token, since, until, rule):
    raw_data = fetch_insights(
        act_id=act_id,
        token=token,
        since=since,
        until=until,
        level=rule["level"],
        time_increment=rule["time_increment"],
        fields=rule["fields"]
    )

    rows = []

    if rule["sheet_key"] == "meta_re_mo":
        for item in raw_data:
            month = to_month(item.get("date_start"))
            reach = to_int(item.get("reach"))
            rows.append([month, reach])

        rows.sort(key=lambda x: x[0], reverse=True)

    elif rule["sheet_key"] == "meta_re_day":
        for item in raw_data:
            day = item.get("date_start", "")
            month = to_month(day)
            reach = to_int(item.get("reach"))
            rows.append([month, day, reach])

        rows.sort(key=lambda x: x[1], reverse=True)

    elif rule["sheet_key"] == "meta_re_ad":
        for item in raw_data:
            month = to_month(item.get("date_start"))
            campaign_name = item.get("campaign_name", "")
            adset_name = item.get("adset_name", "")
            ad_name = item.get("ad_name", "")
            reach = to_int(item.get("reach"))
            rows.append([month, campaign_name, adset_name, ad_name, reach])

        rows.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=True)

    print(f"Built rows: {len(rows)}")
    if rows:
        print(f"Sample row: {rows[0]}")

    return rows


def fetch_insights(act_id, token, since, until, level, time_increment, fields):
    url = f"https://graph.facebook.com/{API_VERSION}/{act_id}/insights"

    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps({
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d")
        }),
        "fields": ",".join(fields),
        "time_increment": time_increment,
        "limit": 5000
    }

    all_rows = []

    while True:
        try:
            response = requests.get(url, params=params, timeout=120)
            print(f"Meta API status: {response.status_code}")
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Meta API request error: {repr(e)}")
            break

        if "error" in data:
            print(f"Meta API error: {json.dumps(data['error'], ensure_ascii=False)}")
            break

        batch = data.get("data", [])
        all_rows.extend(batch)

        paging = data.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break

        url = next_url
        params = None

    print(f"Fetched rows: {len(all_rows)}")
    return all_rows


def write_to_sheet(spreadsheet, sheet_name, header, rows):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"Worksheet open error ({sheet_name}): {repr(e)}")
        return

    try:
        worksheet.clear()
        output = [header] + rows
        worksheet.update("A1", output)
        print(f"Write success: {sheet_name} ({len(rows)} rows)")
    except Exception as e:
        print(f"Write error ({sheet_name}): {repr(e)}")


def to_month(date_str):
    if not date_str:
        return ""
    return date_str[:7]


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    main()
