import unittest
from pathlib import Path
from src.holiday_settlement.context import load_context, summarize, validate_context

class ContextTest(unittest.TestCase):
    def test_fixture(self):
        value = load_context(Path("fixtures/context.json"))
        self.assertEqual(value["domain"], "holiday-settlement")
        self.assertIn("参与方", summarize(value))
    def test_missing(self):
        value = load_context(Path("fixtures/context.json")).copy()
        value.pop("actors")
        with self.assertRaisesRegex(ValueError, "缺少字段"):
            validate_context(value)
    def test_bad_items(self):
        value = load_context(Path("fixtures/context.json")).copy()
        value["constraints"] = ["有效", ""]
        with self.assertRaisesRegex(ValueError, "非空文本"):
            validate_context(value)

if __name__ == "__main__":
    unittest.main()
