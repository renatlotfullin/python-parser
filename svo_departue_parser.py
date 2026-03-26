import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import pandas as pd
import requests


OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def prev_day() -> Tuple[datetime, str]:
    d = datetime.now() - timedelta(days=1)
    return d.replace(hour=0, minute=0, second=0, microsecond=0), d.strftime("%Y-%m-%d")


def parse_iso_dt(dt_raw: Optional[str]) -> Optional[datetime]:
    if not dt_raw:
        return None
    try:
        return datetime.fromisoformat(dt_raw)
    except Exception:
        return None


def hhmm_from_iso(dt_raw: Optional[str]) -> Optional[str]:
    dt = parse_iso_dt(dt_raw)
    return dt.strftime("%H:%M") if dt else None


def fetch_all_departures_from_api(base_date: datetime) -> List[Dict]:
    """
    Полная выдача вылетов за день из API табло Шереметьево.
    """
    date_start = base_date.strftime("%Y-%m-%dT00:00:00+03:00")
    date_end = (base_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+03:00")
    url = "https://www.svo.aero/bitrix/timetable/"
    params = {
        "direction": "departure",
        "dateStart": date_start,
        "dateEnd": date_end,
        "perPage": 9999,
        "page": 0,
        "locale": "ru",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    resp = requests.get(url, params=params, headers=headers, timeout=90)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    items = payload.get("items") or []

    out: List[Dict] = []
    for item in items:
        co = item.get("co") or {}
        code = normalize_space(str(co.get("code") or ""))
        flight_id = normalize_space(str(item.get("flt") or ""))
        if not code or not flight_id:
            continue

        mar2 = item.get("mar2") or {}
        mar1 = item.get("mar1") or {}
        destination = normalize_space(
            str(mar2.get("city") or mar2.get("description") or mar2.get("airport") or "")
        )
        departure_airport_name = normalize_space(
            str(
                mar1.get("airport_rus")
                or mar1.get("airport")
                or mar1.get("description")
                or ""
            )
        )
        airport_name = normalize_space(
            str(
                mar2.get("airport_rus")
                or mar2.get("airport")
                or mar2.get("description")
                or ""
            )
        )

        planned_dt_raw = item.get("t_st")
        actual_dt_raw = item.get("t_otpr")
        planned_dt = parse_iso_dt(planned_dt_raw)
        actual_dt = parse_iso_dt(actual_dt_raw)

        out.append(
            {
                "airport_code": "SVO",
                "flight_number": f"{code}{flight_id}",
                "airline": code,
                "flight_id": flight_id,
                "destination": destination,
                "departure_airport_name": departure_airport_name,
                "airport_name": airport_name,
                "planned_departure_time": hhmm_from_iso(planned_dt_raw),
                "actual_departure_time": hhmm_from_iso(actual_dt_raw),
                "planned_departure_datetime": planned_dt.strftime("%Y-%m-%d %H:%M") if planned_dt else None,
                "actual_departure_datetime": actual_dt.strftime("%Y-%m-%d %H:%M") if actual_dt else None,
            }
        )
    return out


def run() -> None:
    base_date, base_ymd = prev_day()
    snapshot_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_file_all = OUTPUT_DIR / "svo_departures_all.csv"

    all_rows = fetch_all_departures_from_api(base_date=base_date)
    new_df = pd.DataFrame(all_rows)
    if new_df.empty:
        print("Новых данных не получено, существующие файлы не изменены.")
        return

    new_df["snapshot_ts"] = snapshot_ts

    existing_all_df = pd.DataFrame()
    if output_file_all.exists():
        try:
            existing_all_df = pd.read_csv(output_file_all)
        except Exception:
            existing_all_df = pd.DataFrame()

    df_all = pd.concat([existing_all_df, new_df], ignore_index=True)
    df_all.drop_duplicates(
        subset=[
            "snapshot_ts",
            "flight_number",
            "destination",
            "planned_departure_datetime",
            "actual_departure_datetime",
        ],
        inplace=True,
    )
    df_all.sort_values(
        by=["snapshot_ts", "actual_departure_datetime", "flight_number"],
        inplace=True,
        na_position="last",
    )
    df_all.to_csv(output_file_all, index=False, encoding="utf-8-sig")

    print(f"Сохранено: {output_file_all} | строк (накопительно): {len(df_all)}")


if __name__ == "__main__":
    run()
