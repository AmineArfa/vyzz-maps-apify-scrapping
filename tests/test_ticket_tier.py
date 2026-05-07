"""Canonical industry → ticket_tier mapping."""
from __future__ import annotations

import unittest

from leadgen.ticket_tier import LOW, MID, TIERS, compute_ticket_tier


class TicketTierTests(unittest.TestCase):
    def test_low_industries_map_to_low(self):
        for ind in [
            "Restaurants and Bars", "Hotels and Leisure", "Med Spa",
            "Retail Stores", "Food and Beverage Brands", "Home Goods",
            "Personal Care Products", "Apparel and Footwear",
            "Consumer Products", "Consumer Services",
        ]:
            with self.subTest(industry=ind):
                self.assertEqual(compute_ticket_tier(ind), "low")
                self.assertIn(ind, LOW)

    def test_mid_industries_map_to_mid(self):
        for ind in ["Clinic Services", "Medical Laboratories", "Dentist"]:
            with self.subTest(industry=ind):
                self.assertEqual(compute_ticket_tier(ind), "mid")
                self.assertIn(ind, MID)

    def test_unmapped_industry_defaults_high(self):
        for ind in [
            "Business Consulting", "Immigration Lawyer", "Interior Design",
            "Personal Injury Lawyer", "Family Lawyer",
            "Real Estate and Trust Lawyer", "Elder and Disabled Care",
        ]:
            with self.subTest(industry=ind):
                self.assertEqual(compute_ticket_tier(ind), "high")

    def test_null_or_empty_returns_none(self):
        self.assertIsNone(compute_ticket_tier(None))
        self.assertIsNone(compute_ticket_tier(""))
        self.assertIsNone(compute_ticket_tier("   "))

    def test_low_and_mid_disjoint(self):
        self.assertEqual(LOW & MID, set())

    def test_tiers_are_low_mid_high(self):
        self.assertEqual(set(TIERS), {"low", "mid", "high"})


if __name__ == "__main__":
    unittest.main()
