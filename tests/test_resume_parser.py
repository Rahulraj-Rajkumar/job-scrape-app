from __future__ import annotations

import unittest
from datetime import datetime

from src.resume_parser import _extract_yoe


class ResumeParserTests(unittest.TestCase):
    def test_extract_yoe_uses_end_year_for_completed_role(self) -> None:
        text = "\n".join(
            [
                "Experience",
                "Walmart",
                "Software Engineer",
                "2021 - 2025",
            ]
        )

        self.assertEqual(_extract_yoe(text), 4)

    def test_extract_yoe_uses_current_year_for_present_role(self) -> None:
        text = "\n".join(
            [
                "Experience",
                "Walmart",
                "Software Engineer",
                "2021 - Present",
            ]
        )

        expected_yoe = datetime.now().year - 2021
        self.assertEqual(_extract_yoe(text), expected_yoe)

    def test_extract_yoe_merges_overlapping_date_ranges(self) -> None:
        text = "\n".join(
            [
                "Experience",
                "Walmart - Software Engineer - 2021 - 2023",
                "Walmart - Senior Software Engineer - 2022 - 2025",
            ]
        )

        self.assertEqual(_extract_yoe(text), 4)


if __name__ == "__main__":
    unittest.main()
