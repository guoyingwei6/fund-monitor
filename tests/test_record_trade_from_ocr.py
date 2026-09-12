import unittest
from unittest.mock import patch
from datetime import datetime, timedelta

from record_trade_from_ocr import CST, Fund, can_update_duplicate, find_duplicate, parse_date, parse_trade


class RecordTradeFromOcrTest(unittest.TestCase):
    def test_parse_trade_keeps_confirmed_share_out_of_fee(self) -> None:
        text = (
            "记录详情\n"
            "南方基金管理股份有限公司\n"
            "50.00元\n"
            "买入成功\n"
            "买入信息\n"
            "买入产品\n"
            "买入金额\n"
            "付款方式\n"
            "买入时间\n"
            "南方纳斯达克100指数(QDII)A\n"
            "50.00元\n"
            "余额宝\n"
            "2026-06-25 14:45:36\n"
            "确认信息\n"
            "确认金额\n"
            "确认份额\n"
            "确认净值\n"
            "手续费\n"
            "确认时间\n"
            "49.94元\n"
            "21.72份\n"
            "2.2994\n"
            "0.06元\n"
            "2026-06-29\n"
            "订单号\n"
            "20260625001080012204430025600510\n"
        )
        funds = [
            Fund(
                page_id="page-id",
                code="160213",
                name="南方纳斯达克100指数(QDII)A",
                shares=0,
            )
        ]

        trade = parse_trade(text, funds)

        self.assertEqual(trade.amount, -50.0)
        self.assertEqual(trade.confirmed_amount, 49.94)
        self.assertEqual(trade.share_delta, 21.72)
        self.assertEqual(trade.nav, 2.2994)
        self.assertEqual(trade.fee, 0.06)

    def test_parse_trade_rejects_share_unit_as_fee(self) -> None:
        text = (
            "南方纳斯达克100指数(QDII)A\n"
            "买入金额\n"
            "50.00元\n"
            "手续费\n"
            "21.72份\n"
            "确认份额\n"
            "21.72份\n"
            "确认净值\n"
            "2.2994\n"
            "买入时间\n"
            "2026-06-25 14:45:36\n"
        )
        funds = [
            Fund(
                page_id="page-id",
                code="160213",
                name="南方纳斯达克100指数(QDII)A",
                shares=0,
            )
        ]

        trade = parse_trade(text, funds)

        self.assertIsNone(trade.fee)

    def test_parse_trade_rejects_implausible_fee_without_unit(self) -> None:
        text = (
            "南方纳斯达克100指数(QDII)A\n"
            "买入金额\n"
            "50.00元\n"
            "手续费\n"
            "21.7215\n"
            "确认份额\n"
            "21.7215\n"
            "确认净值\n"
            "2.2994\n"
            "买入时间\n"
            "2026-06-25 14:45:36\n"
        )
        funds = [
            Fund(
                page_id="page-id",
                code="160213",
                name="南方纳斯达克100指数(QDII)A",
                shares=0,
            )
        ]

        trade = parse_trade(text, funds)

        self.assertIsNone(trade.fee)

    def test_parse_trade_handles_vision_ocr_units_from_screenshot(self) -> None:
        text = (
            "17:20 1\n"
            ":!!! 5GA Q\n"
            "<\n"
            "记录详情\n"
            "◎ 南方基金管理股份有限公司\n"
            "50.00元\n"
            "买入成功\n"
            "买入信息\n"
            "买入产品\n"
            "买入金额\n"
            "付款方式\n"
            "买入时间\n"
            "南方纳斯达克100指数（QDII）A〉\n"
            "50.00元\n"
            "余额宝\n"
            "2026-06-25 14:45:36\n"
            "确认信息\n"
            "确认金额\n"
            "确认份额\n"
            "确认净值\n"
            "手续费\n"
            "确认时间\n"
            "49.941\n"
            "21.7215\n"
            "2.2994\n"
            "0.06J\n"
            "2026-06-29\n"
            "订单号\n"
            "20260625001080012204430025600510\n"
            "再买一笔\n"
        )
        funds = [
            Fund(
                page_id="page-id",
                code="160213",
                name="南方纳斯达克100指数(QDII)A",
                shares=0,
            )
        ]

        trade = parse_trade(text, funds)

        self.assertEqual(trade.confirmed_amount, 49.94)
        self.assertEqual(trade.share_delta, 21.72)
        self.assertEqual(trade.fee, 0.06)

    def test_find_duplicate_matches_old_natural_key_when_order_key_is_new(self) -> None:
        fund = Fund(
            page_id="fund-page-id",
            code="016452",
            name="南方纳斯达克100指数A",
            shares=0,
        )
        trade = parse_trade(
            (
                "南方纳斯达克100指数A\n"
                "买入金额\n"
                "50.00元\n"
                "确认份额\n"
                "21.72份\n"
                "确认净值\n"
                "2.2994\n"
                "手续费\n"
                "0.06元\n"
                "买入时间\n"
                "2026-06-25 14:45:36\n"
                "订单号\n"
                "20260625001080012204430025600510\n"
            ),
            [fund],
        )
        old_page = {
            "id": "old-page-id",
            "properties": {"状态": {"select": {"name": "已确认"}}},
        }

        with patch("record_trade_from_ocr.notion_post") as notion_post:
            notion_post.side_effect = [
                {"results": []},
                {"results": []},
                {"results": [old_page]},
            ]

            match = find_duplicate(trade)

        self.assertIsNotNone(match)
        self.assertEqual(match.page["id"], "old-page-id")
        self.assertEqual(match.kind, "natural_key")
        self.assertTrue(can_update_duplicate(match, trade))

    def test_parse_date_ignores_future_full_text_fallback(self) -> None:
        future_date = (datetime.now(CST).date() + timedelta(days=10)).isoformat()
        parsed = parse_date(f"确认时间\n{future_date}", fallback="2026-06-30")

        self.assertEqual(parsed, "2026-06-30")


if __name__ == "__main__":
    unittest.main()
