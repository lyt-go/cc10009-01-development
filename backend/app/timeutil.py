"""时间解析工具：心跳、租约、批次边界都按 UTC 比较。"""
from __future__ import annotations

from datetime import datetime, timezone

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_time(value: object, default: datetime | None = None) -> datetime | None:
    """把 ISO 字符串、日期串或 datetime 统一成带时区的 UTC 时间。

    不带时区的输入按 UTC 解释；只写日期的输入按当天 00:00 UTC 处理。
    解析失败时返回 default，由调用方决定是拒绝还是取当前时间。
    """
    if value is None or value == "":
        return default
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return default
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return default
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def to_int(value: object, default: int | None = None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default
