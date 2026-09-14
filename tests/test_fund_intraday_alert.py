import os
import json
import tempfile
import unittest
from datetime import datetime, date
from unittest.mock import patch, MagicMock

from fund_intraday_alert import (
    CST,
    is_trading_hours,
    is_cn_trade_day,
    send_bark_alert,
    load_state,
    save_state,
    export_quotes_snapshot,
    fetch_sina_quotes,
    fetch_realtime_quotes,
)


class FundIntradayAlertTest(unittest.TestCase):
    def test_is_trading_hours_weekends(self) -> None:
        # 2026-09-12 为周六
        dt_sat = datetime(2026, 9, 12, 10, 30, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_sat)
        self.assertFalse(is_trading)
        self.assertEqual(reason, "周末休市")

    def test_is_trading_hours_workday_sessions(self) -> None:
        # 2026-09-11 为周五
        # 10:00 早盘
        dt_morning = datetime(2026, 9, 11, 10, 0, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_morning)
        self.assertTrue(is_trading)
        self.assertEqual(reason, "早盘时段")

        # 12:00 午休
        dt_lunch = datetime(2026, 9, 11, 12, 0, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_lunch)
        self.assertFalse(is_trading)

        # 14:00 午盘
        dt_afternoon = datetime(2026, 9, 11, 14, 0, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_afternoon)
        self.assertTrue(is_trading)
        self.assertEqual(reason, "午盘时段")

        # 18:00 盘后
        dt_night = datetime(2026, 9, 11, 18, 0, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_night)
        self.assertFalse(is_trading)

    def test_is_trading_hours_monday_morning(self) -> None:
        # 2026-09-14 为周一，10:00 属于正常早盘时段
        dt_mon = datetime(2026, 9, 14, 10, 0, tzinfo=CST)
        is_trading, reason = is_trading_hours(dt_mon)
        self.assertTrue(is_trading)
        self.assertEqual(reason, "早盘时段")

    def test_is_cn_trade_day_weekend(self) -> None:
        sat = date(2026, 9, 12)
        self.assertFalse(is_cn_trade_day(sat))

    @patch("fund_intraday_alert.requests.post")
    def test_send_bark_alert_post_success(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        with patch("fund_intraday_alert.BARK_URL", "https://bark.example.com/TEST_KEY/Body"):
            ok = send_bark_alert("测试标题", "测试内容")
            self.assertTrue(ok)
            mock_post.assert_called_once()
            call_args, call_kwargs = mock_post.call_args
            self.assertEqual(call_args[0], "https://bark.example.com/push")
            json_data = call_kwargs["json"]
            self.assertEqual(json_data["device_key"], "TEST_KEY")
            self.assertEqual(json_data["title"], "测试标题")
            self.assertEqual(json_data["body"], "测试内容")
            self.assertEqual(json_data["url"], "https://guoyingwei6.github.io/fund-monitor/")

    @patch("fund_intraday_alert.requests.get")
    @patch("fund_intraday_alert.requests.post")
    def test_send_bark_alert_post_fallback_to_get(
        self, mock_post: MagicMock, mock_get: MagicMock
    ) -> None:
        # 模拟 POST 失败，降级为 GET
        mock_post.side_effect = Exception("POST connection error")
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get.return_value = mock_get_resp

        with patch("fund_intraday_alert.BARK_URL", "https://bark.example.com/TEST_KEY/Body"):
            ok = send_bark_alert("标题", "内容")
            self.assertTrue(ok)
            mock_get.assert_called_once()
            self.assertEqual(mock_get.call_args[1]["params"]["url"], "https://guoyingwei6.github.io/fund-monitor/")

    def test_state_save_and_load(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            tmp_path = tf.name

        try:
            with patch("fund_intraday_alert.STATE_FILE", tmp_path):
                state = {"000015:up": {"date": "2026-09-11", "rate": 1.5}}
                save_state(state)
                loaded = load_state()
                self.assertEqual(loaded, state)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @patch("fund_intraday_alert.requests.get")
    def test_fetch_sina_quotes(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.text = 'var hq_str_fu_022459="易方达中证A500ETF联接A,10:38:00,1.2293,1.2319,1.2319,-0.1138,-0.2122,2026-09-14,1.2268,-0.4137";'
        mock_get.return_value = mock_resp

        quotes = fetch_sina_quotes(["022459"])
        self.assertIn("022459", quotes)
        q = quotes["022459"]
        self.assertEqual(q["code"], "022459")
        self.assertEqual(q["name"], "易方达中证A500ETF联接A")
        self.assertEqual(q["est_rate"], -0.2122)
        self.assertEqual(q["est_nav"], 1.2293)
        self.assertEqual(q["gztime"], "2026-09-14 10:38:00")
        self.assertEqual(q["nav"], 1.2319)
        self.assertEqual(q["nav_date"], "2026-09-14")

    @patch("fund_intraday_alert.fetch_sina_quotes")
    @patch("fund_intraday_alert.fetch_eastmoney_quotes")
    def test_fetch_realtime_quotes_fallback_to_sina(
        self, mock_eastmoney: MagicMock, mock_sina: MagicMock
    ) -> None:
        # 东财返回 GSZZL 为空（est_rate 为 None）且 gztime 为空
        mock_eastmoney.return_value = {
            "022459": {
                "code": "022459",
                "name": "易方达中证A500",
                "est_nav": None,
                "est_rate": None,
                "gztime": "",
                "nav": 1.2319,
                "nav_date": "2026-09-11",
            }
        }
        # 新浪返回盘中实时估值与时间
        mock_sina.return_value = {
            "022459": {
                "code": "022459",
                "name": "易方达中证A500ETF联接A",
                "est_nav": 1.2273,
                "est_rate": -0.37,
                "gztime": "2026-09-14 10:15:00",
                "nav": 1.2319,
                "nav_date": "",
            }
        }

        quotes = fetch_realtime_quotes(["022459"])
        self.assertIn("022459", quotes)
        q = quotes["022459"]
        # 验证新浪补充了盘中估值与估值时间，同时保留了东财的净值信息
        self.assertEqual(q["est_rate"], -0.37)
        self.assertEqual(q["est_nav"], 1.2273)
        self.assertEqual(q["gztime"], "2026-09-14 10:15:00")
        self.assertEqual(q["nav"], 1.2319)
        self.assertEqual(q["nav_date"], "2026-09-11")

    def test_export_quotes_snapshot(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            tmp_path = tf.name

        try:
            sample = {
                "022459": {
                    "code": "022459",
                    "name": "易方达中证A500ETF联接A",
                    "est_rate": -0.25,
                    "est_nav": 1.2288,
                }
            }
            export_quotes_snapshot(sample, tmp_path)
            with open(tmp_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            self.assertIn("quotes", payload)
            self.assertEqual(payload["quotes"]["022459"]["est_rate"], -0.25)
            self.assertEqual(payload["source"], "cloud-actions")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    unittest.main()
