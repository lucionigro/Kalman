import tempfile
import unittest
from pathlib import Path

from kalman_quant.config import load_config


class ConfigSafetyTests(unittest.TestCase):
    def test_live_profile_is_blocked_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "live.json"
            path.write_text('{"profile":"live","mode":"live","enabled":false}', encoding="utf-8")
            with self.assertRaises(RuntimeError):
                load_config(str(path))


if __name__ == "__main__":
    unittest.main()
