"""
基金监控脚本 - 每日自动更新 Notion 净值与再平衡建议
数据来源: 天天基金（东方财富）公开接口
"""

import os
import re
import json
import time
import requests
from datetime import datetime, date

# ── 配置 ──────────────────────────────────────────────
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID = "25bf49a100364b528fcf8c84077c338a"

# 再平衡触发阈值：实际配比偏离目标超过此值时触发建议
# 例如 0.05 = 5%（目标30%，实际>35%或<25%时触发）
REBALANCE_THRESHOLD = float(os.environ.get("REBALANCE_THRESHOLD", "0.05"))

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

EASTMONEY_HEADERS = {
    "Referer": "http://fund.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# ── 天天基金 API ───────────────────────────────────────

def fetch_fund_nav(fund_code: str) -> dict | None:
    """
    获取基金最新确认净值（T+1，收盘后约18:00可用）
    返回: {nav, acc_nav, change_rate(%), nav_date}
    """
    url = "https://api.fund.eastmoney.com/f10/lsjz"
    params = {
        "fundCode": fund_code,
        "pageIndex": 1,
        "pageSize": 2,
        "startDate": "",
        "endDate": "",
        "callback": "jQuery",
    }
    try:
        resp = requests.get(url, params=params, headers=EASTMONEY_HEADERS, timeout=10)
        resp.raise_for_status()
        text = resp.text
        json_str = text[text.index("(") + 1 : text.rindex(")")]
        data = json.loads(json_str)
        records = data.get("Data", {}).get("LSJZList", [])
        if not records:
            return None
        latest = records[0]
        return {
            "nav": float(latest["DWJZ"]),
            "acc_nav": float(latest["LJJZ"]),
            "change_rate": float(latest["JZZZL"]),  # 百分比值，如 1.53
            "nav_date": latest["FSRQ"],
        }
    except Exception as e:
        print(f"  [警告] 获取 {fund_code} 净值失败: {e}")
        return None


def fetch_realtime_estimate(fund_code: str) -> dict | None:
    """
    获取盘中估算净值（交易日9:30-15:00有效，有一定误差）
    返回: {est_nav, est_change_rate(%), update_time}
    """
    url = f"http://fundgz.1234567.com.cn/js/{fund_code}.js"
    try:
        resp = requests.get(url, headers=EASTMONEY_HEADERS, timeout=10)
        resp.raise_for_status()
        text = resp.text
        json_str = text[text.index("(") + 1 : text.rindex(")")]
        data = json.loads(json_str)
        return {
            "est_nav": float(data.get("gsz", 0)),
            "est_change_rate": float(data.get("gszzl", 0)),
            "update_time": data.get("gztime", ""),
        }
    except Exception as e:
        print(f"  [警告] 获取 {fund_code} 实时估值失败: {e}")
        return None


# ── Notion 读写 ────────────────────────────────────────

def get_notion_funds() -> list:
    """读取 Notion 数据库中所有基金持仓"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    resp = requests.post(url, headers=NOTION_HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    funds = []
    for page in data.get("results", []):
        props = page["properties"]

        def get_text(key):
            items = props.get(key, {}).get("rich_text", [])
            return items[0]["plain_text"] if items else ""

        def get_title(key):
            items = props.get(key, {}).get("title", [])
            return items[0]["plain_text"] if items else ""

        def get_number(key):
            return props.get(key, {}).get("number") or 0

        fund_code = get_text("基金代码").strip()
        fund_name = get_title("基金名称").strip()
        shares = get_number("持有份额")
        target_pct_raw = get_text("目标占比").strip()

        # 解析目标占比，支持 "30%"、"0.3"、"30" 等格式
        target_pct = 0.0
        if target_pct_raw:
            num = re.sub(r"[%\s]", "", target_pct_raw)
            try:
                val = float(num)
                target_pct = val / 100 if val > 1 else val
            except ValueError:
                pass

        if not fund_name:
            continue

        # 读取已有的现有资产（用于无代码基金，如余利宝）
        existing_value = get_number("现有资产")

        funds.append({
            "page_id": page["id"],
            "fund_code": fund_code,
            "fund_name": fund_name,
            "shares": shares,
            "target_pct": target_pct,
            "existing_value": existing_value,
        })

    return funds


def update_notion_fund(
    page_id: str,
    nav: float,
    change_rate: float,
    current_value: float,
    daily_pnl: float,
    suggestion: str,
    rebalance_amount: float,
    nav_date: str,
    target_pct: float = 0,
    total_value: float = 0,
) -> bool:
    """更新单只基金在 Notion 中的数据"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    now_str = f"{nav_date} {datetime.now().strftime('%H:%M')}"

    # 今日涨跌幅: Notion percent 格式存储小数（0.0153 显示为 1.53%）
    change_rate_stored = change_rate / 100

    # 目标差值 = 现有资产 - 目标金额（负值=低配，正值=超配）
    target_deviation = round(current_value - (target_pct * total_value), 2)

    # 配置比例: Notion percent 格式存储小数（0.30 显示为 30%）
    allocation_ratio = (current_value / total_value) if total_value > 0 else 0

    props = {
        "当前净值": {"number": round(nav, 4)},
        "今日涨跌幅": {"number": change_rate_stored},
        "现有资产": {"number": round(current_value, 2)},
        "今日盈亏": {"number": round(daily_pnl, 2)},
        "配置比例": {"number": round(allocation_ratio, 4)},
        "目标差值": {"number": target_deviation},
        "操作建议": {"select": {"name": suggestion}},
        "更新时间": {"rich_text": [{"text": {"content": now_str}}]},
    }

    resp = requests.patch(url, headers=NOTION_HEADERS, json={"properties": props}, timeout=15)
    return resp.status_code == 200


# ── 指数估值 ───────────────────────────────────────────

# 沪深300 PE 参考阈值（历史中位数约13）
HS300_THRESHOLDS = {"low": 12, "high": 18}
# 中证A500 PE 参考阈值（对标中证500，历史中位数约27）
A500_THRESHOLDS = {"low": 20, "high": 35}


def fetch_index_pe(index_name: str) -> float | None:
    """
    用 akshare 获取指数最新 PE(TTM)
    index_name: "沪深300" 或 "中证500"
    """
    try:
        import akshare as ak
        df = ak.stock_index_pe_lg(symbol=index_name)
        if df.empty:
            return None
        latest = df.iloc[-1]
        # 优先用滚动市盈率(TTM)，兼容不同版本列名
        for col in ["滚动市盈率", "等权滚动市盈率", "整体法市盈率", df.columns[-1]]:
            if col in df.columns:
                val = latest[col]
                if val and str(val) not in ("-", "nan", "None"):
                    return round(float(val), 2)
    except Exception as e:
        print(f"  [警告] 获取 {index_name} PE失败: {e}")
    return None


def pe_signal(pe: float, thresholds: dict) -> str:
    """根据 PE 值返回估值信号"""
    if pe <= thresholds["low"]:
        return "低估"
    elif pe >= thresholds["high"]:
        return "高估"
    else:
        return "正常"


def update_market_callout(hs300_pe: float | None, a500_pe: float | None):
    """将市场温度追加到数据库描述区（标题下方，表格上方）"""
    today = date.today().strftime("%Y-%m-%d")

    pe_parts = []
    if hs300_pe:
        signal = pe_signal(hs300_pe, HS300_THRESHOLDS)
        pe_parts.append(f"沪深300 PE {hs300_pe} [{signal}]")
    if a500_pe:
        signal = pe_signal(a500_pe, A500_THRESHOLDS)
        pe_parts.append(f"中证A500 PE {a500_pe} [{signal}]")
    pe_str = "  |  ".join(pe_parts) if pe_parts else "估值数据暂时不可用"
    market_line = f"📊 市场温度 {today}：{pe_str}"

    # 读取现有描述，去掉上次写的市场温度行，保留其余内容
    db_resp = requests.get(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=NOTION_HEADERS, timeout=10
    )
    existing_desc = ""
    if db_resp.status_code == 200:
        existing_desc = "".join(
            b.get("plain_text", "") for b in db_resp.json().get("description", [])
        )

    strategy_lines = [l for l in existing_desc.splitlines() if not l.startswith("📊 市场温度")]
    strategy_text = "\n".join(strategy_lines).strip()
    new_desc = f"{strategy_text}\n{market_line}" if strategy_text else market_line

    resp = requests.patch(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=NOTION_HEADERS,
        json={"description": [{"type": "text", "text": {"content": new_desc}}]},
        timeout=10,
    )
    status = "OK" if resp.status_code == 200 else f"FAIL({resp.status_code})"
    print(f"  [{status}] 市场温度已写入描述区")
    print(f"       {market_line}")


# ── 再平衡逻辑 ─────────────────────────────────────────

def calculate_rebalancing(funds: list, total_value: float) -> list:
    """
    计算每只基金的再平衡建议
    逻辑：
      - |实际占比 - 目标占比| > REBALANCE_THRESHOLD → 触发建议
      - 超配 → 建议卖出，卖出金额 = 偏离金额
      - 低配 → 建议买入，买入金额 = 偏离金额
    """
    result = []
    for fund in funds:
        current_value = fund.get("current_value", 0)
        target_pct = fund.get("target_pct", 0)
        current_pct = current_value / total_value if total_value > 0 else 0
        deviation = current_pct - target_pct  # 正值=超配，负值=低配
        rebalance_amount = deviation * total_value

        if target_pct == 0:
            suggestion = "待确认"
            rebalance_amount = 0
        elif abs(deviation) <= REBALANCE_THRESHOLD:
            suggestion = "持有"
        elif deviation > 0:
            suggestion = "建议卖出"
        else:
            suggestion = "建议买入"

        result.append({
            **fund,
            "current_pct": current_pct,
            "deviation": deviation,
            "suggestion": suggestion,
            "rebalance_amount": rebalance_amount,
        })

    return result


def print_summary(funds: list, total_value: float, total_daily_pnl: float):
    """打印汇总报告"""
    print("\n" + "=" * 68)
    print(f"  基金组合日报  {date.today()}  {datetime.now().strftime('%H:%M')}")
    print("=" * 68)
    print(f"  总市值:   ¥{total_value:>12,.2f}")
    sign = "+" if total_daily_pnl >= 0 else "-"
    print(f"  今日盈亏: {sign}¥{abs(total_daily_pnl):>11,.2f}")
    print("-" * 68)
    print(f"  {'基金名称':<14} {'净值':>7} {'今日涨跌':>9} {'市值':>11} {'实配':>6} {'目标':>6}  建议")
    print("-" * 68)

    need_rebalance = False
    for f in funds:
        cr = f.get("change_rate", 0)
        arrow = "+" if cr >= 0 else "-"
        pct_str = f"{arrow}{abs(cr):.2f}%"
        suggestion = f.get("suggestion", "")
        if suggestion in ("建议买入", "建议卖出"):
            need_rebalance = True
            amt = f.get("rebalance_amount", 0)
            sug_str = f"{suggestion} ¥{abs(amt):,.0f}"
        else:
            sug_str = suggestion

        print(
            f"  {f['fund_name'][:13]:<14}"
            f"{f.get('nav', 0):>7.4f}"
            f"{pct_str:>10}"
            f"  ¥{f.get('current_value', 0):>9,.0f}"
            f"  {f.get('current_pct', 0)*100:>5.1f}%"
            f"  {f.get('target_pct', 0)*100:>5.1f}%"
            f"  {sug_str}"
        )

    print("=" * 68)
    if need_rebalance:
        print("  [!] 存在再平衡需求，请查看 Notion 操作建议列")
    else:
        print("  [OK] 配置比例正常，无需调仓")
    print()


# ── 主流程 ─────────────────────────────────────────────

def main():
    if not NOTION_TOKEN:
        print("错误: 请设置环境变量 NOTION_TOKEN")
        return

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始更新基金净值...")

    # 1. 读取 Notion 持仓
    funds = get_notion_funds()
    if not funds:
        print("Notion 中未找到基金数据，请检查数据库 ID 和权限")
        return
    print(f"读取到 {len(funds)} 只基金")

    # 2. 逐一获取净值（无代码的现金类基金直接用 Notion 现有资产）
    for fund in funds:
        code = fund["fund_code"]
        if not code:
            # 余利宝等货币基金：净值固定≈1，直接用已有市值，不更新
            fund["nav"] = 1.0
            fund["acc_nav"] = 1.0
            fund["change_rate"] = 0.0
            fund["nav_date"] = str(date.today())
            fund["current_value"] = fund["existing_value"]
            fund["daily_pnl"] = 0.0
            print(f"  跳过净值更新: {fund['fund_name']}（无基金代码，使用现有资产 ¥{fund['existing_value']}）")
            continue

        nav_data = fetch_fund_nav(code)
        time.sleep(0.5)  # 避免请求过快

        if nav_data:
            fund.update(nav_data)
            fund["current_value"] = nav_data["nav"] * fund["shares"]
            fund["daily_pnl"] = fund["current_value"] * nav_data["change_rate"] / 100
        else:
            fund.setdefault("nav", 0)
            fund.setdefault("acc_nav", 0)
            fund.setdefault("change_rate", 0)
            fund.setdefault("nav_date", str(date.today()))
            fund["current_value"] = fund["existing_value"]
            fund["daily_pnl"] = 0.0
            print(f"  净值获取失败: {fund['fund_name']} ({code})，使用现有资产")

    total_value = sum(f["current_value"] for f in funds)
    total_daily_pnl = sum(f["daily_pnl"] for f in funds)

    # 3. 计算再平衡建议
    funds = calculate_rebalancing(funds, total_value)

    # 4. 更新 Notion
    print("正在更新 Notion...")
    for fund in funds:
        if fund.get("nav", 0) == 0 and fund.get("fund_code"):
            continue
        success = update_notion_fund(
            page_id=fund["page_id"],
            nav=fund["nav"],
            change_rate=fund["change_rate"],
            current_value=fund["current_value"],
            daily_pnl=fund["daily_pnl"],
            suggestion=fund["suggestion"],
            rebalance_amount=fund["rebalance_amount"],
            nav_date=fund.get("nav_date", str(date.today())),
            target_pct=fund.get("target_pct", 0),
            total_value=total_value,
        )
        status = "OK" if success else "FAIL"
        print(f"  [{status}] {fund['fund_name']} ({fund['fund_code']})")
        time.sleep(0.3)

    # 5. 获取指数估值并更新 callout
    print("正在获取指数估值...")
    hs300_pe = fetch_index_pe("沪深300")
    a500_pe = fetch_index_pe("中证500")     # 中证A500 暂用中证500 PE 作参考
    update_market_callout(hs300_pe, a500_pe)

    # 6. 打印汇总
    print_summary(funds, total_value, total_daily_pnl)


if __name__ == "__main__":
    main()
