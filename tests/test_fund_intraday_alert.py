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


if __name__ == "__main__":
    unittest.main()
