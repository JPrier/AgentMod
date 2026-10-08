import unittest

from parse import parse


class Parse(unittest.TestCase):
    def test_pairs(self):
        self.assertEqual(parse("a=1; b=2"), {"a": "1", "b": "2"})

    def test_trailing_separator(self):
        self.assertEqual(parse("a=1;"), {"a": "1"})

    def test_empty(self):
        self.assertEqual(parse(""), {})


if __name__ == "__main__":
    unittest.main()
