import unittest

from textutil import title_case


class TitleCase(unittest.TestCase):
    def test_words(self):
        self.assertEqual(title_case("hello there"), "Hello There")


if __name__ == "__main__":
    unittest.main()
