"""校验视觉模型的结构化字段，然后复用现有Notion字段写入。"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

import record_trade_from_ocr as ledger


SCHEMA = json.loads(Path(__file__).with_name("image_trade.schema.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


def validate_fields(data: dict) -> None:
    errors = list(VALIDATOR.iter_errors(data))
    if errors:
        # 不打印ValidationError.message，它可能包含完整的个人数据。
        fields = sorted({".".join(map(str, e.path)) or "根对象" for e in errors})
        raise ValueError("字段格式不正确: " + ", ".join(fields))
    if data["issues"]:
        raise ValueError("模型标记了识别疑问，请查看识别预览并核对原图")
    if not data["fund_code"] and not data["fund_name"]:
        raise ValueError("缺少明确的基金代码或完整名称")
    if not data["date"] or date.fromisoformat(data["date"]) > datetime.now(ledger.CST).date():
        raise ValueError("缺少有效日期或日期在未来，不允许猜测日期")
    kind, status = data["type"], data["status"]
    if kind not in ("持仓快照", "买入", "卖出"):
        raise ValueError("不支持的页面类型")
    if status not in (("已同步",) if kind == "持仓快照" else ("待确认", "已确认")):
        raise ValueError("页面类型与确认状态不一致")
    if data["amount"] is None or Decimal(data["amount"]) <= 0:
        raise ValueError("金额必须明确且大于零；零持仓清仓快照需人工核对")
    if kind != "持仓快照" and not data["order_no"]:
        raise ValueError("交易截图缺少订单号，无法可靠去重")
    if status == "待确认":
        if any(data[k] is not None for k in ("shares", "nav", "nav_date")):
            raise ValueError("待确认交易不能包含已确认份额或净值")
        return
    if any(data[k] is None for k in ("shares", "nav", "nav_date")):
        raise ValueError("缺少份额、对应净值或净值日期，无法交叉核对")
    nav_day = date.fromisoformat(data["nav_date"])
    if nav_day > datetime.now(ledger.CST).date():
        raise ValueError("净值日期在未来")
    if kind == "持仓快照" and data["date"] != data["nav_date"]:
        raise ValueError("持仓金额与净值日期不同，不能交叉核对")
    if kind != "持仓快照" and data["nav_date"] < data["date"]:
        raise ValueError("确认日期早于申请日期")
    shares, nav, amount = (Decimal(data[k]) for k in ("shares", "nav", "amount"))
    if shares <= 0 or nav <= 0:
        raise ValueError("份额和净值必须大于零")
    if shares.as_tuple().exponent > -2:
        raise ValueError("份额需保留至少两位小数，否则无法可靠核对展示精度")
    if nav.as_tuple().exponent > -3:
        raise ValueError("净值精度不足或小数点疑似缺失，请提供完整净值")
    if kind == "持仓快照":
        if data["fee"] is not None or data["order_no"] is not None:
            raise ValueError("持仓快照不应带有交易手续费或订单号")
        expected = amount
    else:
        if data["fee"] is None:
            raise ValueError("缺少明确手续费，不能假设为零")
        fee = Decimal(data["fee"])
        if fee >= amount:
            raise ValueError("手续费不应大于或等于金额")
        expected = amount - fee if kind == "买入" else amount + fee
    # 容差只来自页面展示精度：净值末位、份额末位及金额分位的舍入。
    tolerance = (shares * Decimal(10) ** nav.as_tuple().exponent
                 + nav * Decimal(10) ** shares.as_tuple().exponent) / 2 + Decimal("0.01")
    if abs(shares * nav - expected) > tolerance:
        raise ValueError("份额×净值与金额/手续费不一致，拒绝写入；请核对原图")


def parse_image_trade(data: dict, funds: list[ledger.Fund]) -> ledger.ParsedTrade:
    validate_fields(data)
    matches = [f for f in funds
               if (not data["fund_code"] or f.code == data["fund_code"])
               and (not data["fund_name"] or ledger.compact(f.name) == ledger.compact(data["fund_name"]))]
    if len(matches) != 1:
        raise ValueError("基金代码/完整名称不能唯一匹配持仓，禁止模糊匹配")
    snapshot = data["type"] == "持仓快照"
    amount = float(data["amount"])
    shares = None if data["shares"] is None else float(data["shares"])
    trade = ledger.ParsedTrade(
        fund=matches[0], trade_type=data["type"], status=data["status"], date=data["date"],
        amount=0 if snapshot else (-amount if data["type"] == "买入" else amount),
        confirmed_amount=amount if data["status"] == "已确认" else None,
        share_delta=None if snapshot or shares is None else (-shares if data["type"] == "卖出" else shares),
        share_snapshot=shares if snapshot else None,
        nav=None if data["nav"] is None else float(data["nav"]),
        fee=None if data["fee"] is None else float(data["fee"]),
        confirm_date=None if snapshot else data["nav_date"], order_no=data["order_no"],
        holding_amount=amount if snapshot else None,
        holding_cost=None, holding_profit=None, holding_profit_rate=None,
        pending_amount=amount if data["status"] == "待确认" else None,
        daily_change=None, source="支付宝截图", dedupe_key="",
    )
    trade.dedupe_key = ledger.build_dedupe_key(trade)
    return trade


def find_duplicate(trade: ledger.ParsedTrade) -> dict | None:
    key = "订单号" if trade.order_no else "去重键"
    value = trade.order_no or trade.dedupe_key
    result = ledger.notion_post(
        f"https://api.notion.com/v1/databases/{ledger.TRADES_DATABASE_ID}/query",
        {"filter": {"property": key, "rich_text": {"equals": value}}, "page_size": 2},
    )
    if len(result["results"]) > 1:
        raise ValueError("已有多条重复记录，请先人工核对，未修改数据")
    return result["results"][0] if result["results"] else None


def check_snapshot_freshness(trade: ledger.ParsedTrade) -> None:
    # 没有日内时间戳时，同日不同记录无法排序，明确拒绝覆盖。
    newer = ledger.query_first_trade({"and": [
        {"property": "基金", "relation": {"contains": trade.fund.page_id}},
        {"property": "日期", "date": {"on_or_after": trade.date}},
        {"property": "状态", "select": {"does_not_equal": "待确认"}},
        {"property": "去重键", "rich_text": {"does_not_equal": trade.dedupe_key}},
    ]})
    if newer:
        raise ValueError("存在同日其他记录或更新记录，无法证明快照较新；拒绝覆盖持仓")


def import_trade(trade: ledger.ParsedTrade, data: dict, no_update_holdings: bool) -> None:
    duplicate = find_duplicate(trade)
    snapshot = trade.trade_type == "持仓快照"
    if snapshot and not no_update_holdings:
        check_snapshot_freshness(trade)
    if duplicate:
        old = duplicate["properties"]
        fund_ids = [item["id"] for item in old.get("基金", {}).get("relation", [])]
        if (fund_ids != [trade.fund.page_id]
                or ledger.select_name(duplicate, "类型") != trade.trade_type
                or (old.get("日期", {}).get("date") or {}).get("start") != trade.date
                or ledger.number_value(old, "金额") != round(trade.amount, 2)):
            raise ValueError("相同订单/去重键对应的基金、类型、日期或金额冲突，未修改数据")
        old_status = ledger.select_name(duplicate, "状态")
        if old_status != "待确认":
            for key, value in (("持有份额快照", trade.share_snapshot), ("份额变动", trade.share_delta), ("确认净值", trade.nav)):
                if old.get(key, {}).get("number") != value:
                    raise ValueError("重复记录的份额或净值不同，需人工复核，未修改数据")
        elif trade.status == "已确认":
            ledger.update_trade_page(duplicate["id"], trade, json.dumps(data, ensure_ascii=False))
    else:
        ledger.create_trade_page(trade, json.dumps(data, ensure_ascii=False))
    if snapshot and not no_update_holdings:
        # 绝对份额覆盖可重试；记录创建成功但此处失败时，重跑不会累加。
        ledger.update_fund_shares_if_needed(trade)
        print("[OK] 快照已记录，持仓已更新")
    else:
        print("[OK] 流水已记录或已存在；未更新持仓，请以持仓快照同步份额")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--json-file")
    source.add_argument("--event-file", help="GitHub事件文件，不将识别数据放进Actions环境日志")
    parser.add_argument("--validate-only", action="store_true", help="只校验字段和等式，不访问Notion")
    parser.add_argument("--dry-run", action="store_true", help="读取Notion核对基金，不写入")
    parser.add_argument("--no-update-holdings", action="store_true")
    args = parser.parse_args()
    content = Path(args.event_file or args.json_file).read_text(encoding="utf-8")
    if args.event_file:
        content = json.loads(content)["inputs"]["trade_json"]
    data = json.loads(content)
    validate_fields(data)
    if args.validate_only:
        print("[OK] 字段与等式校验通过，未连接Notion；尚未验证基金匹配或识图准确率")
        return 0
    if not ledger.NOTION_TOKEN:
        raise ValueError("未设置NOTION_TOKEN")
    trade = parse_image_trade(data, ledger.load_funds())
    if args.dry_run:
        print("[OK] 字段、等式与基金匹配通过，未写入Notion；未检查历史记录冲突")
        return 0
    import_trade(trade, data, args.no_update_holdings)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        # JSON解码错误不回显输入正文，其余错误都是上面的固定说明。
        message = "JSON内容无效" if isinstance(exc, json.JSONDecodeError) else str(exc)
        print(f"[ERROR] {message}", file=sys.stderr)
        raise SystemExit(2)
