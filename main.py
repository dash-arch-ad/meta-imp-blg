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
            "start_date": since.strftime("%Y-%m-%d"),
            "end_date": until.strftime("%Y-%m-%d"),
            "page": page,
            "page_size": page_size,
            "query_lifetime": "false",
            "enable_total_metrics": "false",
            "multi_adv_report_in_utc_time": "false",
        }

        if dimensions:
            params["dimensions"] = json.dumps(dimensions, separators=(",", ":"))
        if metrics:
            params["metrics"] = json.dumps(metrics, separators=(",", ":"))

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
