import unittest
from unittest.mock import patch, MagicMock

from fund_monitor import (
    safe_float,
    fetch_fund_nav,
    calculate_rebalancing,
    valuation_signal,
    sync_parent_page_strategy_callout,
    PAGES_URL,
    HS300_PE_THRESHOLDS,
    HS300_PB_THRESHOLDS,
)


class FundMonitorTest(unittest.TestCase):
    def test_safe_float(self) -> None:
        self.assertEqual(safe_float("1.234"), 1.234)
        self.assertEqual(safe_float(12.5), 12.5)
        self.assertEqual(safe_float("", default=0.0), 0.0)
        self.assertEqual(safe_float(None, default=0.0), 0.0)
        self.assertEqual(safe_float("invalid", default=0.0), 0.0)

    @patch("fund_monitor.requests.get")
    def test_fetch_fund_nav_with_empty_growth_rate(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = 'jQuery({"Data":{"LSJZList":[{"DWJZ":"1.2345","LJJZ":"1.5678","JZZZL":"","FSRQ":"2026-09-11"}]}})'
        mock_get.return_value = mock_resp

        data = fetch_fund_nav("000015")
        self.assertIsNotNone(data)
        self.assertEqual(data["nav"], 1.2345)
        self.assertEqual(data["acc_nav"], 1.5678)
        self.assertEqual(data["change_rate"], 0.0)
        self.assertEqual(data["nav_date"], "2026-09-11")

    def test_calculate_rebalancing_buy_and_sell(self) -> None:
        funds = [
            {
                "fund_code": "000015",
                "fund_name": "华夏纯债",
                "current_value": 3000.0,
                "target_pct": 0.40,
            },
            {
                "fund_code": "022459",
                "fund_name": "中证A500",
                "current_value": 7000.0,
                "target_pct": 0.60,
            },
        ]
        total_value = 10000.0
        results = calculate_rebalancing(funds, total_value)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["suggestion"], "建议买入")
        self.assertAlmostEqual(results[0]["deviation"], -0.10)
        self.assertAlmostEqual(results[0]["rebalance_amount"], -1000.0)

        self.assertEqual(results[1]["suggestion"], "建议卖出")
        self.assertAlmostEqual(results[1]["deviation"], 0.10)
        self.assertAlmostEqual(results[1]["rebalance_amount"], 1000.0)

    def test_calculate_rebalancing_hold(self) -> None:
        funds = [
            {
                "fund_code": "000015",
                "fund_name": "华夏纯债",
                "current_value": 4020.0,
                "target_pct": 0.40,
            },
        ]
        results = calculate_rebalancing(funds, 10000.0)
        self.assertEqual(results[0]["suggestion"], "持有")

    def test_valuation_signal(self) -> None:
        sig_low = valuation_signal(10.0, 1.0, HS300_PE_THRESHOLDS, HS300_PB_THRESHOLDS)
        self.assertEqual(sig_low, "低估")

        sig_mid = valuation_signal(15.0, 1.5, HS300_PE_THRESHOLDS, HS300_PB_THRESHOLDS)
        self.assertEqual(sig_mid, "正常")

        sig_high = valuation_signal(22.0, 2.2, HS300_PE_THRESHOLDS, HS300_PB_THRESHOLDS)
        self.assertEqual(sig_high, "高估")

    def test_default_strategy_description_includes_pages_url(self) -> None:
        from fund_monitor import DEFAULT_STRATEGY_DESCRIPTION, PAGES_URL
        self.assertIn("盘中估值看板", DEFAULT_STRATEGY_DESCRIPTION)
        self.assertIn(PAGES_URL, DEFAULT_STRATEGY_DESCRIPTION)

    @patch("fund_monitor.requests.patch")
    @patch("fund_monitor.requests.get")
    def test_sync_parent_page_creates_embed_block_when_missing(
        self, mock_get: MagicMock, mock_patch: MagicMock
    ) -> None:
        db_resp = MagicMock(status_code=200)
        db_resp.json.return_value = {"parent": {"page_id": "parent_page_001"}}

        children_resp = MagicMock(status_code=200)
        children_resp.json.return_value = {
            "results": [
                {
                    "id": "block_desc_001",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"plain_text": "🎯 长期投资配置方案："}]},
                }
            ]
        }
        mock_get.side_effect = [db_resp, children_resp]

        patch_resp = MagicMock(status_code=200)
        patch_resp.json.return_value = {"results": [{"id": "new_block_id"}]}
        mock_patch.return_value = patch_resp

        sync_parent_page_strategy_callout("更新后的策略说明")

        # 应该调用两次 patch：一次更新说明，一次创建 embed 预览块
        self.assertEqual(mock_patch.call_count, 2)
        embed_call_args = mock_patch.call_args_list[1]
        json_body = embed_call_args.kwargs.get("json", {})
        self.assertIn("children", json_body)
        self.assertEqual(json_body["children"][0]["type"], "embed")
        self.assertEqual(json_body["children"][0]["embed"]["url"], PAGES_URL)
        self.assertEqual(json_body.get("after"), "block_desc_001")

    @patch("fund_monitor.requests.patch")
    @patch("fund_monitor.requests.get")
    def test_sync_parent_page_skips_embed_when_already_exists(
        self, mock_get: MagicMock, mock_patch: MagicMock
    ) -> None:
        db_resp = MagicMock(status_code=200)
        db_resp.json.return_value = {"parent": {"page_id": "parent_page_001"}}

        children_resp = MagicMock(status_code=200)
        children_resp.json.return_value = {
            "results": [
                {
                    "id": "block_desc_001",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"plain_text": "🎯 长期投资配置方案："}]},
                },
                {
                    "id": "block_embed_001",
                    "type": "embed",
                    "embed": {"url": PAGES_URL},
                },
            ]
        }
        mock_get.side_effect = [db_resp, children_resp]

        patch_resp = MagicMock(status_code=200)
        mock_patch.return_value = patch_resp

        sync_parent_page_strategy_callout("更新后的策略说明")

        # 已存在 embed 块时，只更新说明，不重复创建 embed
        self.assertEqual(mock_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
