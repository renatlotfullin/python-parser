import csv
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Tuple

from urllib.parse import urlencode
from urllib.request import urlopen


# ID курсов, как в stepik_courses_table.py
COURSE_IDS: List[int] = [
    244062,
    223260,
    220868,
    219647,
    219191,
    215033,
    209880,
    209760,
    209479,
    201431,
    199534,
    192863,
    185696,
    175185,
    173117,
    163258,
    159477,
    153842,
    146872,
    144530,
    138385,
    119119,
]

API_BASE = "https://stepik.org/api"
CSV_PATH = Path("stepik_courses_daily.csv")


def get_json(url: str) -> dict:
    with urlopen(url) as resp:
        return json.load(resp)


def fetch_courses(ids: List[int]) -> List[dict]:
    params = [("ids[]", cid) for cid in ids]
    url = f"{API_BASE}/courses?{urlencode(params)}"
    data = get_json(url)
    return data["courses"]


def fetch_review_summaries(summary_ids: List[int]) -> Dict[int, Tuple[float, int]]:
    """
    Возвращает словарь:
      review_summary_id -> (average_rating, reviews_count)
    """
    if not summary_ids:
        return {}

    params = [("ids[]", sid) for sid in summary_ids]
    url = f"{API_BASE}/course-review-summaries?{urlencode(params)}"
    data = get_json(url)
    summaries = data.get("course-review-summaries", [])

    result: Dict[int, Tuple[float, int]] = {}
    for s in summaries:
        avg = s.get("average") or 0.0
        count = s.get("count") or 0
        result[s["id"]] = (float(avg), int(count))
    return result


def read_last_totals() -> Dict[int, Tuple[int, int]]:
    """
    Читает последний зафиксированный total по каждому курсу из CSV.
    Возвращает:
      course_id -> (last_learners_total, last_reviews_total)
    Если файл отсутствует, возвращает пустой словарь.
    """
    if not CSV_PATH.exists():
        return {}

    last_rows: Dict[int, Tuple[int, int]] = {}

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cid = int(row["course_id"])
                learners_total = int(row["learners_total"])
                reviews_total = int(row["reviews_total"])
            except (KeyError, ValueError):
                continue

            last_rows[cid] = (learners_total, reviews_total)

    return last_rows


def append_today_stats() -> None:
    today = dt.date.today().isoformat()

    courses = fetch_courses(COURSE_IDS)

    # ID сводок отзывов для всех курсов
    summary_ids = [
        c["review_summary"]
        for c in courses
        if c.get("review_summary") is not None
    ]

    reviews_info = fetch_review_summaries(summary_ids)
    last_totals = read_last_totals()

    file_exists = CSV_PATH.exists()

    with CSV_PATH.open("a", encoding="utf-8-sig", newline="") as f:
        fieldnames = [
            "date",
            "course_id",
            "course_title",
            "learners_total",
            "learners_delta",
            "reviews_total",
            "reviews_delta",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        # Если файл создаётся впервые — пишем заголовок
        if not file_exists:
            writer.writeheader()

        for c in courses:
            cid = int(c["id"])
            title = str(c.get("title", "")).replace("\t", " ")
            learners_total = int(c.get("learners_count") or 0)

            summary_id = c.get("review_summary")
            _, reviews_total = reviews_info.get(summary_id, (0.0, 0))

            last_learners, last_reviews = last_totals.get(
                cid, (learners_total, reviews_total)
            )

            learners_delta = learners_total - last_learners
            reviews_delta = reviews_total - last_reviews

            writer.writerow(
                {
                    "date": today,
                    "course_id": cid,
                    "course_title": title,
                    "learners_total": learners_total,
                    "learners_delta": learners_delta,
                    "reviews_total": reviews_total,
                    "reviews_delta": reviews_delta,
                }
            )


def main() -> None:
    append_today_stats()


if __name__ == "__main__":
    main()
