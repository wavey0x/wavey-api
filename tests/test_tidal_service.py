import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from services.tidal import TidalError, TidalService


STRATEGY_ADDRESS = "0x1111111111111111111111111111111111111111"
WANT_ADDRESS = "0x2222222222222222222222222222222222222222"
TOKEN_ADDRESS = "0x3333333333333333333333333333333333333333"


@contextmanager
def _fake_connect():
    yield object()


def _make_service(*, deploy_require_curve_quote):
    with patch("services.tidal.setup_web3", return_value=MagicMock()):
        service = TidalService(
            "dummy.db",
            5000,
            deploy_require_curve_quote=deploy_require_curve_quote,
        )

    service._connect = _fake_connect
    service._warn_if_not_wal = MagicMock()
    service._load_strategy_deploy_context = MagicMock(
        return_value={
            "strategyAddress": STRATEGY_ADDRESS,
            "strategyName": "Test Strategy",
            "auctionAddress": None,
            "wantAddress": WANT_ADDRESS,
            "wantSymbol": "WANT",
            "active": True,
            "balances": [],
        }
    )
    service._select_deploy_balance = MagicMock(
        return_value={
            "tokenAddress": TOKEN_ADDRESS,
            "tokenSymbol": "SELL",
            "rawBalance": "1000000000000000000",
            "normalizedBalance": "1.0",
            "priceUsd": "10.0",
            "usdValue": "10.0",
        }
    )
    service._quote_token = MagicMock(
        return_value={
            "amountOutRaw": 2_500_000_000,
            "tokenOutDecimals": 6,
            "providerStatuses": {"curve": "error", "1inch": "ok"},
            "providerAmounts": {"curve": 0, "1inch": 2_500_000_000},
            "requestUrl": "https://prices.example/v1/quote",
        }
    )
    service._compute_starting_price = MagicMock(return_value=2750)
    service._find_matching_auctions = MagicMock(return_value=[])
    service._build_deploy_salt = MagicMock(return_value="0x" + ("11" * 32))
    service._build_create_auction_tx = MagicMock(return_value=(None, "0xdeadbeef"))
    return service


class TidalServiceDeployCurveQuoteTests(unittest.TestCase):
    def test_build_strategy_deploy_tx_warns_when_curve_quote_is_missing(self):
        service = _make_service(deploy_require_curve_quote=False)

        payload = service.build_strategy_deploy_tx(STRATEGY_ADDRESS)

        self.assertEqual(
            payload["warnings"],
            ["Curve quote unavailable for deploy inference (status: error)"],
        )
        self.assertFalse(payload["inference"]["curveQuoteAvailable"])
        self.assertEqual(payload["inference"]["curveQuoteStatus"], "error")
        self.assertEqual(payload["inference"]["quoteAmountOutRaw"], "2500000000")

    def test_build_strategy_deploy_tx_can_still_require_curve_quote(self):
        service = _make_service(deploy_require_curve_quote=True)

        with self.assertRaisesRegex(
            TidalError,
            r"Curve quote unavailable for deploy inference \(status: error\)",
        ):
            service.build_strategy_deploy_tx(STRATEGY_ADDRESS)


if __name__ == "__main__":
    unittest.main()
