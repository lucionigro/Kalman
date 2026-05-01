import tempfile
import unittest
from pathlib import Path

from kalman_quant.models import OrderIntent, utc_now_iso
from kalman_quant.storage import SQLiteStore


class StorageTests(unittest.TestCase):
    def test_records_order_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStore(str(Path(tmp) / "test.db"))
            try:
                store.record_intent(
                    OrderIntent(
                        ticker="AMD",
                        timestamp=utc_now_iso(),
                        action="BUY",
                        quantity=2,
                        estimated_price=100.0,
                        mode="dry_run",
                        reason="test",
                        risk_checks=["ok"],
                    )
                )
                rows = store.latest_order_intents()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["ticker"], "AMD")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
