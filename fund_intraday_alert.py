"""
基金盘中异动监控脚本 - 云端自动化定时检测与 Bark 实时推送
运行环境: GitHub Actions (每 15-20 分钟自动调度)
数据源: 东方财富移动端盘中实时估值接口
"""

import os
import json
import re
import time
import urllib.parse
from datetime import datetime, date, timezone, timedelta
import requests

# ── 基础配置与时区 ────────────────────────────────────
CST = timezone(timedelta(hours=8))

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "25bf49a100364b528fcf8c84077c338a").strip()
BARK_URL = os.environ.get("BARK_URL", "").strip()
PAGES_URL = os.environ.get("PAGES_URL", "https://guoyingwei6.github.io/fund-monitor/").strip()

# 涨跌幅触发阈值（百分比），股票层默认 ±1.5%，债券层默认 ±0.5%
STOCK_ALERT_THRESHOLD = float(os.environ.get("STOCK_ALERT_THRESHOLD", "1.5"))
BOND_ALERT_THRESHOLD = float(os.environ.get("BOND_ALERT_THRESHOLD", "0.5"))

# 是否强制检查（用于测试或工作流手动触发，不检查休市）
FORCE_CHECK = os.environ.get("FORCE_CHECK", "false").lower() in ("1", "true", "yes")

STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state.json")

EASTMONEY_HEADERS = {
    "Referer": "http://fund.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


# ── 交易时间与交易日判定 ──────────────────────────────

def is_cn_trade_day(target_date: date) -> bool:
    """判断今天是否为 A 股交易日（轻量化检查，无需 akshare）。"""
    if target_date.weekday() >= 5:
        return False
    try:
        resp = requests.get(f"https://timor.tech/api/holiday/info/{target_date.strftime('%Y-%m-%d')}", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            holiday_type = data.get("type", {}).get("type")
            # 1: 周末, 2: 节假日
            if holiday_type in (1, 2):
                return False
    except Exception as e:
        pass
    return True


def is_trading_hours(now: datetime) -> tuple[bool, str]:
    """判断当前时间是否处于交易时段。"""
    if now.weekday() >= 5:
        return False, "周末休市"

    minutes = now.hour * 60 + now.minute
    # 早盘 09:25 - 11:35 (包含开盘集合竞价与缓冲)
    if 565 <= minutes <= 695:
        return True, "早盘时段"
    # 午盘 12:55 - 15:05 (包含午间准备与收盘缓冲)
    if 775 <= minutes <= 905:
        return True, "午盘时段"

    return False, f"非交易时段 ({now.strftime('%H:%M')})"


# ── Notion 读取 ────────────────────────────────────────

def get_notion_funds() -> list[dict]:
    """从 Notion 数据库读取当前所有持仓基金。"""
    if not NOTION_TOKEN:
        print("[错误] 未配置 NOTION_TOKEN")
        return []

    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, headers=headers, json={"page_size": 100}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[错误] 查询 Notion 数据库失败: {e}")
        return []

    funds = []
    for page in data.get("results", []):
        props = page.get("properties", {})

        def get_text(key):
            items = props.get(key, {}).get("rich_text", [])
            return items[0]["plain_text"].strip() if items else ""

        def get_title(key):
            items = props.get(key, {}).get("title", [])
            return items[0]["plain_text"].strip() if items else ""

        def get_select(key):
            return (props.get(key, {}).get("select") or {}).get("name", "")

        fund_code = get_text("基金代码")
        fund_name = get_title("基金名称")
        asset_layer = get_select("资产层")

        if fund_code and fund_name:
            funds.append({
                "fund_code": fund_code,
                "fund_name": fund_name,
                "asset_layer": asset_layer,
            })

    return funds


# ── 实时估值获取（东财 + 新浪双源互补） ────────────────

def fetch_eastmoney_quotes(codes: list[str]) -> dict[str, dict]:
    """批量拉取东方财富移动端实时估值。"""
    if not codes:
        return {}

    url = "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo"
    params = {
        "Fcodes": ",".join(codes),
        "pageIndex": "1",
        "pageSize": str(len(codes)),
        "Sort": "",
        "SortColumn": "",
        "IsShowSE": "false",
        "P": "F",
        "deviceid": "gh-actions-alert",
        "plat": "Iphone",
        "product": "EFund",
        "version": "6.2.8",
    }
    try:
        resp = requests.get(url, params=params, headers=EASTMONEY_HEADERS, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[错误] 请求东方财富估值接口失败: {e}")
        return {}

    if not data or not data.get("Success") or "Datas" not in data:
        return {}

    quotes = {}
    for item in data["Datas"]:
        code = str(item.get("FCODE", "")).strip()
        gszzl = item.get("GSZZL")
        gsz = item.get("GSZ")
        gztime = item.get("GZTIME", "")

        est_rate = None
        if gszzl is not None and gszzl != "":
            try:
                est_rate = float(gszzl)
            except ValueError:
                pass

        est_nav = None
        if gsz is not None and gsz != "":
            try:
                est_nav = float(gsz)
            except ValueError:
                pass

        quotes[code] = {
            "code": code,
            "name": str(item.get("SHORTNAME", "")).strip(),
            "est_nav": est_nav,
            "est_rate": est_rate,
            "gztime": gztime,
            "nav": float(item["NAV"]) if item.get("NAV") else None,
            "nav_date": item.get("PDATE", ""),
        }
    return quotes


def fetch_sina_quotes(codes: list[str]) -> dict[str, dict]:
    """批量拉取新浪财经实时估值接口 (fu_{code})。"""
    if not codes:
        return {}

    list_param = ",".join([f"fu_{code}" for code in codes])
    url = f"https://hq.sinajs.cn/list={list_param}"
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "gbk"
        text = resp.text
    except Exception as e:
        print(f"[错误] 请求新浪估值接口失败: {e}")
        return {}

    quotes = {}
    pattern = re.compile(r'var hq_str_fu_(\d+)="([^"]*)";')
    for match in pattern.finditer(text):
        code = match.group(1)
        raw_val = match.group(2).strip()
        if not raw_val:
            continue
        parts = raw_val.split(",")
        if len(parts) < 8:
            continue
        try:
            est_nav = float(parts[2]) if parts[2] else None
        except ValueError:
            est_nav = None

        try:
            nav = float(parts[3]) if parts[3] else None
        except ValueError:
            nav = None

        try:
            est_rate = float(parts[6]) if parts[6] else None
        except ValueError:
            est_rate = None

        date_str = parts[7].strip() if len(parts) > 7 else ""
        time_str = parts[1].strip() if len(parts) > 1 else ""
        gztime = f"{date_str} {time_str}".strip() if (date_str or time_str) else ""

        quotes[code] = {
            "code": code,
            "name": parts[0].strip(),
            "est_nav": est_nav,
            "est_rate": est_rate,
            "gztime": gztime,
            "nav": nav,
            "nav_date": date_str,
        }
    return quotes


def fetch_realtime_quotes(codes: list[str]) -> dict[str, dict]:
    """批量拉取实时估值（东财为主，新浪互补容灾）。"""
    if not codes:
        return {}

    quotes = fetch_eastmoney_quotes(codes)
    missing_codes = [
        c for c in codes
        if c not in quotes or quotes[c].get("est_rate") is None or not quotes[c].get("gztime")
    ]
    if missing_codes:
        sina_quotes = fetch_sina_quotes(missing_codes)
        for c, s_q in sina_quotes.items():
            if c not in quotes:
                quotes[c] = s_q
            else:
                em_q = quotes[c]
                if em_q.get("est_rate") is None and s_q.get("est_rate") is not None:
                    em_q["est_rate"] = s_q["est_rate"]
                if em_q.get("est_nav") is None and s_q.get("est_nav") is not None:
                    em_q["est_nav"] = s_q["est_nav"]
                if not em_q.get("gztime") and s_q.get("gztime"):
                    em_q["gztime"] = s_q["gztime"]
                if not em_q.get("name") and s_q.get("name"):
                    em_q["name"] = s_q["name"]
                if not em_q.get("nav") and s_q.get("nav"):
                    em_q["nav"] = s_q["nav"]

    return quotes


# ── 状态防重 ──────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[警告] 保存告警状态文件失败: {e}")


# ── Bark 推送 ─────────────────────────────────────────

def send_bark_alert(title: str, body: str, url: str | None = None) -> bool:
    """向 Bark 推送消息，优先使用 POST JSON，失败回退为 GET。"""
    if not BARK_URL:
        print("[警告] 未配置 BARK_URL，跳过推送")
        return False

    target_url = url or PAGES_URL
    parsed = urllib.parse.urlparse(BARK_URL)
    key = [part for part in parsed.path.split("/") if part][0] if parsed.path else ""
    if not key:
        print("[错误] BARK_URL 中未找到有效 Key")
        return False

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    post_url = f"{base_url}/push"
    payload = {
        "device_key": key,
        "title": title,
        "body": body,
        "group": "基金盘中提醒",
        "level": "timeSensitive",
        "icon": "https://www.eastmoney.com/favicon.ico",
    }
    if target_url:
        payload["url"] = target_url

    try:
        resp = requests.post(post_url, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
    except Exception as e:
        print(f"[提示] Bark POST 推送失败，尝试 GET 回退: {e}")

    get_endpoint = f"{base_url}/{urllib.parse.quote(key)}/{urllib.parse.quote(title)}/{urllib.parse.quote(body)}"
    params = {
        "group": "基金盘中提醒",
        "level": "timeSensitive",
        "icon": "https://www.eastmoney.com/favicon.ico",
    }
    if target_url:
        params["url"] = target_url

    try:
        resp = requests.get(get_endpoint, params=params, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"[错误] 发送 Bark 失败: {e}")
        return False


# ── 主逻辑 ─────────────────────────────────────────────

def main():
    now = datetime.now(CST)
    today_str = now.strftime("%Y-%m-%d")
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 启动基金盘中监控检测...")

    if not FORCE_CHECK:
        if not is_cn_trade_day(now.date()):
            print(f"[休市] 今天 ({today_str}) 不是 A 股交易日，退出。")
            return

        is_trading, reason = is_trading_hours(now)
        if not is_trading:
            print(f"[休市] 当前不在交易时段: {reason}，退出。")
            return
    else:
        print("[强制] FORCE_CHECK=true，跳过交易时段检查。")

    # 1. 获取 Notion 基金
    funds = get_notion_funds()
    if not funds:
        print("[提示] 未读取到 Notion 基金列表。")
        return

    print(f"[读取] 找到 {len(funds)} 只 Notion 监控基金。")
    codes = [f["fund_code"] for f in funds]

    # 2. 获取实时估值
    quotes = fetch_realtime_quotes(codes)
    state = load_state()

    # 校验东财返回的估值时间是否为今日，若全部基金均无今日估值则视为实际休市
    has_today_quote = any(
        (q.get("gztime") or "").startswith(today_str)
        for q in quotes.values()
    )
    if not FORCE_CHECK and quotes and not has_today_quote:
        print(f"[休市] 基金实时估值时间未更新为今日 ({today_str})，判定为非交易日或未开市，退出。")
        return

    # 清理非当天的历史状态
    state = {k: v for k, v in state.items() if v.get("date") == today_str}

    alerts_triggered = 0

    for fund in funds:
        code = fund["fund_code"]
        name = fund["fund_name"]
        layer = fund.get("asset_layer", "")
        quote = quotes.get(code)

        if not quote or quote.get("est_rate") is None:
            print(f"  [-] {name} ({code}): 暂无盘中估值")
            continue

        est_rate = quote["est_rate"]
        est_nav = quote["est_nav"] or quote["nav"] or 0.0
        threshold = BOND_ALERT_THRESHOLD if layer == "债券层" else STOCK_ALERT_THRESHOLD

        direction = None
        action_text = ""
        if est_rate >= threshold:
            direction = "up"
            action_text = "考虑适度止盈或暂停买入"
        elif est_rate <= -threshold:
            direction = "down"
            action_text = "考虑逢低买入或定投加仓"

        rate_str = f"+{est_rate:.2f}%" if est_rate > 0 else f"{est_rate:.2f}%"
        print(f"  [>] {name} ({code}): 盘中估值 {rate_str} (阈值 ±{threshold}%)")

        if not direction:
            continue

        # 防重检测：同一方向当天默认只提醒一次，除非涨跌幅又进一步扩大 1.0%
        state_key = f"{code}:{direction}"
        last_alert = state.get(state_key)
        if last_alert:
            last_rate = last_alert.get("rate", 0.0)
            if abs(est_rate - last_rate) < 1.0:
                print(f"      ↳ 已于 {last_alert.get('time')} 提醒过 ({last_rate:+.2f}%)，变动不足 1%，跳过")
                continue

        # 触发推送
        dir_cn = "大涨" if direction == "up" else "大跌"
        title = f"{name} 盘中{dir_cn} {rate_str}"
        body_lines = [
            f"最新估值: {est_nav:.4f} ({rate_str})",
            f"资产分类: {layer or '基金'}",
            f"估值时间: {quote.get('gztime', now.strftime('%H:%M'))}",
            f"操作建议: {action_text}",
        ]
        is_qdii = any(k in name for k in ("QDII", "纳斯达克", "标普", "美股"))
        if is_qdii:
            body_lines.append("特别提醒: 海外标的白天处于休市期，盘中估值为股指期货估算，实际净值以美股收盘为准。")
        if PAGES_URL:
            body_lines.append(f"查看详情: {PAGES_URL}")
        body = "\n".join(body_lines)

        print(f"      ★ 触发提醒: {title}")
        success = send_bark_alert(title, body)
        if success:
            print("      ✓ Bark 推送成功")
            state[state_key] = {
                "date": today_str,
                "time": now.strftime("%H:%M:%S"),
                "rate": est_rate,
            }
            alerts_triggered += 1
        else:
            print("      ✗ Bark 推送失败")

    save_state(state)
    print(f"[完成] 检测结束，共触发并发送 {alerts_triggered} 条提醒。")


if __name__ == "__main__":
    main()
