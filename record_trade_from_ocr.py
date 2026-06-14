"""
Record an Alipay fund trade screenshot into Notion.

Input is OCR text from a screenshot. The script extracts the fund, amount,
date, share snapshot, NAV and other optional fields, writes one transaction
row, and updates the holding share snapshot when it is safe to do so.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CST = timezone(timedelta(hours=8))

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
HOLDINGS_DATABASE_ID = os.environ.get(
    "NOTION_DATABASE_ID", "25bf49a100364b528fcf8c84077c338a"
)
TRADES_DATABASE_ID = os.environ.get(
    "NOTION_TRADES_DATABASE_ID", "b7a9b06a-b7db-43f0-b825-aa8e61af1285"
)

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PATCH"),
        )
    ),
)


@dataclass
class Fund:
    page_id: str
    code: str
    name: str
    shares: float


@dataclass
class ParsedTrade:
    fund: Fund
    trade_type: str
    status: str
    date: str
    amount: float
    confirmed_amount: float | None
    share_delta: float | None
    share_snapshot: float | None
    nav: float | None
    fee: float | None
    confirm_date: str | None
    order_no: str | None
    holding_amount: float | None
    holding_cost: float | None
    holding_profit: float | None
    holding_profit_rate: float | None
    pending_amount: float | None
    daily_change: float | None
    source: str
    dedupe_key: str


def notion_post(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    resp = SESSION.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def notion_patch(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    resp = SESSION.patch(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def plain_text(prop: dict[str, Any], key: str) -> str:
    items = prop.get(key, {}).get("rich_text", [])
    return items[0]["plain_text"].strip() if items else ""


def title_text(prop: dict[str, Any], key: str) -> str:
    items = prop.get(key, {}).get("title", [])
    return items[0]["plain_text"].strip() if items else ""


def number_value(prop: dict[str, Any], key: str) -> float:
    return prop.get(key, {}).get("number") or 0.0


def load_funds() -> list[Fund]:
    data = notion_post(
        f"https://api.notion.com/v1/databases/{HOLDINGS_DATABASE_ID}/query",
        {},
    )
    funds: list[Fund] = []
    for page in data.get("results", []):
        props = page["properties"]
        name = title_text(props, "基金名称")
        code = plain_text(props, "基金代码")
        if name and code:
            funds.append(
                Fund(
                    page_id=page["id"],
                    code=code,
                    name=name,
                    shares=number_value(props, "持有份额"),
                )
            )
    return funds


def normalize_text(text: str) -> str:
    table = str.maketrans({
        "，": ",",
        "。": ".",
        "：": ":",
        "％": "%",
        "￥": "¥",
        "－": "-",
        "—": "-",
        "　": " ",
    })
    return text.translate(table)


def compact(text: str) -> str:
    return re.sub(r"\s+", "", normalize_text(text))


def parse_float(value: str) -> float:
    return float(value.replace(",", "").replace("+", "").strip())


def find_number_after_labels(text: str, labels: list[str]) -> float | None:
    normalized = normalize_text(text)
    joined = compact(text)
    number = r"[-+]?\d+(?:,\d{3})*(?:\.\d+)?"
    for label in labels:
        patterns = [
            rf"(?<!待){re.escape(label)}(?:\([^)]*\)|（[^）]*）)?\s*[:：]?\s*(?:CN¥|CNY|¥|人民币)?\s*({number})",
            rf"(?<!待){re.escape(label)}(?:\([^)]*\)|（[^）]*）)?(?:CN¥|CNY|¥|人民币)?({number})",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized, re.I) or re.search(pattern, joined, re.I)
            if match:
                return parse_float(match.group(1))
    return None


def parse_date(text: str, fallback: str | None = None) -> str:
    normalized = normalize_text(text)
    match = re.search(r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", normalized)
    if match:
        year, month, day = (int(x) for x in match.groups())
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"

    match = re.search(r"(?<!\d)(\d{1,2})[-/.月](\d{1,2})(?!\d)", normalized)
    if match:
        year = datetime.now(CST).year
        month, day = (int(x) for x in match.groups())
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"

    return fallback or datetime.now(CST).date().isoformat()


def find_date_after_labels(text: str, labels: list[str]) -> str | None:
    normalized = normalize_text(text)
    for label in labels:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*(20\d{{2}}[-/.年]\d{{1,2}}[-/.月]\d{{1,2}}(?:\s+\d{{1,2}}:\d{{2}}(?::\d{{2}})?)?)"
        match = re.search(pattern, normalized)
        if match:
            parsed = parse_date(match.group(1), fallback="")
            return parsed or None
    return None


def find_order_no(text: str) -> str | None:
    c = compact(text)
    match = re.search(r"订单号[:：]?(\d{12,})", c)
    if match:
        return match.group(1)
    return None


def infer_trade_type(text: str) -> str:
    c = compact(text)
    if "资产详情" in c and any(word in c for word in ("持有金额", "持有份额", "金额(元)")):
        return "持仓快照"
    if any(word in c for word in ("卖出", "赎回")):
        return "卖出"
    if "分红" in c:
        return "分红"
    if any(word in c for word in ("调整", "校准", "份额修正")):
        return "份额调整"
    return "买入"


def match_fund(text: str, funds: list[Fund]) -> Fund:
    c = compact(text)
    code_matches = re.findall(r"(?<!\d)(\d{6})(?!\d)", c)
    for code in code_matches:
        for fund in funds:
            if fund.code == code:
                return fund

    exact = [fund for fund in funds if compact(fund.name) in c]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise ValueError(f"截图匹配到多只基金: {', '.join(f.name for f in exact)}")

    fuzzy: list[Fund] = []
    for fund in funds:
        tokens = [part for part in re.split(r"[A-Za-z0-9（）()·\-\s]+", fund.name) if len(part) >= 2]
        score = sum(1 for token in tokens if token in c)
        if score >= 2:
            fuzzy.append(fund)
    if len(fuzzy) == 1:
        return fuzzy[0]

    raise ValueError("无法从截图 OCR 文本匹配到基金，请确认截图包含基金代码或完整基金名称")


def build_dedupe_key(trade: ParsedTrade) -> str:
    if trade.order_no:
        return f"order-{trade.order_no}"

    if trade.trade_type == "持仓快照":
        raw = "|".join([
            trade.fund.code,
            trade.trade_type,
            trade.date,
            "" if trade.share_snapshot is None else f"{trade.share_snapshot:.4f}",
            "" if trade.holding_amount is None else f"{trade.holding_amount:.2f}",
        ])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"snapshot-{trade.fund.code}-{trade.date}-{digest}"

    parts = [
        trade.fund.code,
        trade.trade_type,
        trade.date,
        f"{trade.amount:.2f}",
        "" if trade.share_delta is None else f"{trade.share_delta:.4f}",
        "" if trade.share_snapshot is None else f"{trade.share_snapshot:.4f}",
    ]
    raw = "|".join(parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{trade.fund.code}-{trade.trade_type}-{trade.date}-{digest}"


def parse_trade(text: str, funds: list[Fund], date_override: str | None = None) -> ParsedTrade:
    fund = match_fund(text, funds)
    trade_type = infer_trade_type(text)

    amount = find_number_after_labels(text, [
        "买入金额",
        "卖出金额",
        "赎回金额",
        "交易金额",
        "成交金额",
        "确认金额",
        "申请金额",
        "申购金额",
        "定投金额",
        "支付金额",
        "分红金额",
    ])
    pending_amount = find_number_after_labels(text, ["待确认金额", "待确认"])
    if trade_type == "持仓快照":
        amount = 0
    elif amount is None and pending_amount is not None:
        amount = pending_amount
    if amount is None:
        raise ValueError("无法识别交易金额，请确认截图包含买入金额/交易金额/待确认金额等字段")

    if trade_type == "买入":
        amount = -abs(amount)
    elif trade_type in ("卖出", "分红"):
        amount = abs(amount)

    share_snapshot = find_number_after_labels(text, ["持有份额", "当前份额"])
    share_delta = find_number_after_labels(text, ["确认份额", "成交份额", "买入份额", "卖出份额", "份额变动", "新增份额"])
    if share_delta is not None and trade_type == "卖出":
        share_delta = -abs(share_delta)

    nav = find_number_after_labels(text, ["确认净值", "成交净值", "买入净值", "基金净值", "当前净值", "净值"])
    confirmed_amount = None if trade_type == "持仓快照" else find_number_after_labels(text, ["确认金额"])
    fee = find_number_after_labels(text, ["手续费"])
    confirm_date = find_date_after_labels(text, ["确认时间", "确认日期"])
    order_no = find_order_no(text)
    holding_amount = find_number_after_labels(text, ["持有金额", "持有市值", "持仓金额"])
    holding_cost = find_number_after_labels(text, ["持仓成本", "持有成本", "成本"])
    holding_profit = find_number_after_labels(text, ["持有收益"])
    holding_profit_rate = find_number_after_labels(text, ["持有收益率"])
    if holding_profit_rate is not None:
        holding_profit_rate = holding_profit_rate / 100
    daily_change = find_number_after_labels(text, ["日涨跌幅", "今日涨跌幅", "涨跌幅"])
    if daily_change is not None:
        daily_change = daily_change / 100

    if trade_type == "持仓快照":
        status = "已同步"
    else:
        status = "待确认" if pending_amount is not None or "待确认" in compact(text) else "已确认"

    parsed = ParsedTrade(
        fund=fund,
        trade_type=trade_type,
        status=status,
        date=date_override or find_date_after_labels(text, ["买入时间", "卖出时间", "交易时间", "申请时间", "下单时间"]) or parse_date(text),
        amount=amount,
        confirmed_amount=confirmed_amount,
        share_delta=share_delta,
        share_snapshot=share_snapshot,
        nav=nav,
        fee=fee,
        confirm_date=confirm_date,
        order_no=order_no,
        holding_amount=holding_amount,
        holding_cost=holding_cost,
        holding_profit=holding_profit,
        holding_profit_rate=holding_profit_rate,
        pending_amount=pending_amount,
        daily_change=daily_change,
        source="支付宝截图",
        dedupe_key="",
    )
    parsed.dedupe_key = build_dedupe_key(parsed)
    return parsed


def find_duplicate(dedupe_key: str) -> dict[str, Any] | None:
    data = notion_post(
        f"https://api.notion.com/v1/databases/{TRADES_DATABASE_ID}/query",
        {"filter": {"property": "去重键", "rich_text": {"equals": dedupe_key}}},
    )
    results = data.get("results", [])
    return results[0] if results else None


def text_prop(content: str) -> dict[str, Any]:
    return {"rich_text": [{"text": {"content": content[:1900]}}]}


def number_prop(value: float | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {"number": round(value, 6)}


def create_trade_page(trade: ParsedTrade, ocr_text: str) -> str:
    props: dict[str, Any] = {
        "交易名称": {"title": [{"text": {"content": f"{trade.date} {trade.fund.name} {trade.trade_type}"}}]},
        "日期": {"date": {"start": trade.date}},
        "基金": {"relation": [{"id": trade.fund.page_id}]},
        "类型": {"select": {"name": trade.trade_type}},
        "状态": {"select": {"name": trade.status}},
        "金额": {"number": round(trade.amount, 2)},
        "来源": {"select": {"name": trade.source}},
        "去重键": text_prop(trade.dedupe_key),
        "OCR原文": text_prop(ocr_text),
    }
    optional = {
        "份额变动": number_prop(trade.share_delta),
        "持有份额快照": number_prop(trade.share_snapshot),
        "确认净值": number_prop(trade.nav),
        "确认金额": number_prop(trade.confirmed_amount),
        "手续费": number_prop(trade.fee),
        "持有金额快照": number_prop(trade.holding_amount),
        "持仓成本": number_prop(trade.holding_cost),
        "持有收益快照": number_prop(trade.holding_profit),
        "持有收益率快照": number_prop(trade.holding_profit_rate),
        "待确认金额": number_prop(trade.pending_amount),
        "日涨跌幅快照": number_prop(trade.daily_change),
    }
    for key, value in optional.items():
        if value is not None:
            props[key] = value
    if trade.order_no:
        props["订单号"] = text_prop(trade.order_no)
    if trade.confirm_date:
        props["确认日期"] = {"date": {"start": trade.confirm_date}}
    if trade.trade_type == "买入":
        props["买入时间"] = {"date": {"start": trade.date}}

    data = notion_post(
        "https://api.notion.com/v1/pages",
        {"parent": {"database_id": TRADES_DATABASE_ID}, "properties": props},
    )
    return data["id"]


def update_fund_shares_if_needed(trade: ParsedTrade) -> bool:
    if trade.status == "待确认":
        return False

    new_shares: float | None = None
    if trade.share_snapshot is not None:
        new_shares = trade.share_snapshot
    elif trade.share_delta is not None:
        new_shares = trade.fund.shares + trade.share_delta

    if new_shares is None:
        return False

    notion_patch(
        f"https://api.notion.com/v1/pages/{trade.fund.page_id}",
        {"properties": {"持有份额": {"number": round(new_shares, 4)}}},
    )
    return True


def read_input(args: argparse.Namespace) -> str:
    if args.ocr_text:
        return args.ocr_text
    if args.ocr_file:
        with open(args.ocr_file, "r", encoding="utf-8") as f:
            return f.read()
    env_text = os.environ.get("OCR_TEXT", "")
    if env_text:
        return env_text
    raise ValueError("请通过 --ocr-text、--ocr-file 或 OCR_TEXT 环境变量传入 OCR 文本")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ocr-text")
    parser.add_argument("--ocr-file")
    parser.add_argument("--date")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not NOTION_TOKEN:
        print("错误: 请设置 NOTION_TOKEN")
        return 2

    ocr_text = read_input(args)
    funds = load_funds()
    trade = parse_trade(ocr_text, funds, args.date)

    print(json.dumps({
        "基金": trade.fund.name,
        "基金代码": trade.fund.code,
        "类型": trade.trade_type,
        "状态": trade.status,
        "日期": trade.date,
        "金额": trade.amount,
        "确认金额": trade.confirmed_amount,
        "份额变动": trade.share_delta,
        "持有份额快照": trade.share_snapshot,
        "确认净值": trade.nav,
        "手续费": trade.fee,
        "确认日期": trade.confirm_date,
        "订单号": trade.order_no,
        "持有金额快照": trade.holding_amount,
        "持有收益快照": trade.holding_profit,
        "持有收益率快照": trade.holding_profit_rate,
        "去重键": trade.dedupe_key,
    }, ensure_ascii=False, indent=2))

    if args.dry_run:
        return 0

    duplicate = find_duplicate(trade.dedupe_key)
    if duplicate:
        print(f"[SKIP] 已存在相同去重键，跳过导入: {duplicate['id']}")
        return 0

    page_id = create_trade_page(trade, ocr_text)
    updated_shares = update_fund_shares_if_needed(trade)
    print(f"[OK] 已写入交易流水: {page_id}")
    print(f"[OK] 已更新持有份额: {updated_shares}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
