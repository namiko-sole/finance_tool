import os
import time
import pandas as pd

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

def is_fresh(file_path: str, max_age_hours: int = 12) -> bool:
    if not os.path.exists(file_path):
        return False
    max_age_seconds = max_age_hours * 3600
    return (time.time() - os.path.getmtime(file_path)) < max_age_seconds


def update_csv(
    file_path: str,
    fetch_fn,
    label: str,
    read_existing: bool = False,
) -> pd.DataFrame | None:
    if is_fresh(file_path):
        print(f"{label}数据文件较新，已跳过更新")
        if read_existing:
            return pd.read_csv(file_path)
        return None
    df = fetch_fn()
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"{label}数据已保存到 {os.path.basename(file_path)}")
    return df


def fetch_with_retry(fetch_fn, label: str) -> pd.DataFrame:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fetch_fn()
        except Exception as exc:
            last_err = exc
            print(f"{label}获取失败，准备重试 {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise last_err if last_err else RuntimeError(f"{label}获取失败")