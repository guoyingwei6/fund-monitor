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

# 沪深300 参考阈值（历史数据）
HS300_PE_THRESHOLDS = {"low": 12, "high": 18}    # PE: <12 低估, >18 高估
HS300_PB_THRESHOLDS = {"low": 1.2, "high": 1.8}  # PB: <1.2 低估, >1.8 高估
# 中证A500 PE 参考阈值（对标中证500，中小盘溢价更高）
A500_PE_THRESHOLDS = {"low": 20, "high": 35}
# 股债利差阈值（沪深300盈利收益率 - 10年国债收益率，单位%）
# >5% 股票便宜，<2% 股票贵
SPREAD_THRESHOLDS = {"low": 2.0, "high": 5.0}


def fetch_index_pe(index_name: str) -> float | None:
    """获取指数最新 PE(TTM)"""
    try:
        import akshare as ak
        df = ak.stock_index_pe_lg(symbol=index_name)
        if df.empty:
            return None
        latest = df.iloc[-1]
        for col in ["滚动市盈率", "等权滚动市盈率", "整体法市盈率"]:
            if col in df.columns:
                val = latest[col]
                if val and str(val) not in ("-", "nan", "None"):
                    return round(float(val), 2)
    except Exception as e:
        print(f"  [警告] 获取 {index_name} PE 失败: {e}")
    return None


def fetch_index_pb(index_name: str) -> float | None:
    """获取指数最新 PB（市净率）"""
    try:
        import akshare as ak
        df = ak.stock_index_pb_lg(symbol=index_name)
        if df.empty:
            return None
        latest = df.iloc[-1]
        for col in ["市净率", "等权市净率"]:
            if col in df.columns:
                val = latest[col]
                if val and str(val) not in ("-", "nan", "None"):
                    return round(float(val), 2)
    except Exception as e:
        print(f"  [警告] 获取 {index_name} PB 失败: {e}")
    return None


def fetch_bond_yield() -> float | None:
    """获取10年期中国国债收益率（%）"""
    try:
        import akshare as ak
        from datetime import timedelta
        start = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
        df = ak.bond_zh_us_rate(start_date=start)
        if df.empty:
            return None
        df = df.sort_values("日期", ascending=False)
        for _, row in df.iterrows():
            val = row.get("中国国债收益率10年")
            if val and str(val) not in ("-", "nan", "None"):
                try:
                    return round(float(val), 4)
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        print(f"  [警告] 获取国债收益率失败: {e}")
    return None


def _signal(val: float, low: float, high: float,
            low_label="🟢 低估", mid_label="🟡 正常", high_label="🔴 高估") -> str:
    if val <= low:
        return low_label
    elif val >= high:
        return high_label
    return mid_label


def market_overall_signal(pe: float | None, pb: float | None, spread: float | None) -> str:
    """综合 PE、PB、股债利差给出市场操作建议（基于沪深300）"""
    score = 0
    valid = 0
    if pe is not None:
        valid += 1
        if pe <= HS300_PE_THRESHOLDS["low"]:
            score += 1
        elif pe >= HS300_PE_THRESHOLDS["high"]:
            score -= 1
    if pb is not None:
        valid += 1
        if pb <= HS300_PB_THRESHOLDS["low"]:
            score += 1
        elif pb >= HS300_PB_THRESHOLDS["high"]:
            score -= 1
    if spread is not None:
        valid += 1
        if spread >= SPREAD_THRESHOLDS["high"]:
            score += 1
        elif spread <= SPREAD_THRESHOLDS["low"]:
            score -= 1
    if valid == 0:
        return "💡 综合建议：数据不足，暂无建议"
    if score >= 2:
        return "💡 综合建议：🟢 多项低估，适合加大股票配置"
    elif score == 1:
        return "💡 综合建议：🟢 偏低估，可适当增加股票"
    elif score == 0:
        return "💡 综合建议：🟡 估值正常，维持当前配置"
    elif score == -1:
        return "💡 综合建议：🔴 偏高估，适当降低股票配置"
    else:
        return "💡 综合建议：🔴 多项高估，建议减少股票增持债券"


def update_market_callout(hs300_pe, hs300_pb, a500_pe, bond_yield):
    """将市场温度追加到数据库描述区（标题下方，表格上方）"""
    today = date.today().strftime("%Y-%m-%d")

    lines = []
    # 沪深300
    if hs300_pe or hs300_pb:
        parts = ["🏦 沪深300"]
        if hs300_pe:
            parts.append(f"PE {hs300_pe}  {_signal(hs300_pe, HS300_PE_THRESHOLDS['low'], HS300_PE_THRESHOLDS['high'])}")
        if hs300_pb:
            parts.append(f"PB {hs300_pb}  {_signal(hs300_pb, HS300_PB_THRESHOLDS['low'], HS300_PB_THRESHOLDS['high'])}")
        lines.append("  ".join(parts))
    # 中证A500
    if a500_pe:
        sig = _signal(a500_pe, A500_PE_THRESHOLDS["low"], A500_PE_THRESHOLDS["high"])
        lines.append(f"📈 中证A500  PE {a500_pe}  {sig}（参考中证500）")
    # 股债利差
    spread = None
    if hs300_pe and bond_yield:
        spread = round((1 / hs300_pe) * 100 - bond_yield, 2)
        sig = _signal(spread, SPREAD_THRESHOLDS["low"], SPREAD_THRESHOLDS["high"],
                      low_label="🔴 股票偏贵", mid_label="🟡 正常", high_label="🟢 股票便宜")
        lines.append(f"📉 股债利差  {spread}%  {sig}（国债 {bond_yield}%）")
    # 综合建议
    lines.append(market_overall_signal(hs300_pe, hs300_pb, spread))

    market_text = "\n".join(lines) if lines else "估值数据暂时不可用"
    market_line = f"📊 市场温度 {today}：\n{market_text}"

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

    skip_keywords = ("📊 市场温度", "🏦 沪深300", "📈 中证A500", "📉 股债利差", "💡 综合建议",
                     "沪深300 PE", "中证A500 PE")
    strategy_lines = [
        l for l in existing_desc.splitlines()
        if not any(l.startswith(k) for k in skip_keywords)
    ]
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
    hs300_pb = fetch_index_pb("沪深300")
    a500_pe = fetch_index_pe("中证500")     # 中证A500 暂用中证500 PE 作参考
    bond_yield = fetch_bond_yield()
    update_market_callout(hs300_pe, hs300_pb, a500_pe, bond_yield)

    # 6. 打印汇总
    print_summary(funds, total_value, total_daily_pnl)


if __name__ == "__main__":
    main()
