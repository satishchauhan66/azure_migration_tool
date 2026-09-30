#!/usr/bin/env python
# Author: Satish Chauhan
"""Tests for type-ahead searchable picker helpers."""

import importlib.util
import unittest
from pathlib import Path

_PKG = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "searchable_picker",
    _PKG / "gui" / "widgets" / "searchable_picker.py",
)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

filter_choices = _mod.filter_choices
choices_for_typeahead = _mod.choices_for_typeahead


class TestFilterChoices(unittest.TestCase):
    def test_empty_needle_returns_all(self):
        vals = ("Alpha", "Beta", "Gamma")
        self.assertEqual(filter_choices(vals, ""), vals)

    def test_case_insensitive_substring(self):
        vals = ("Sales_Prod", "Sales_QA", "HR_QA")
        self.assertEqual(filter_choices(vals, "qa"), ("Sales_QA", "HR_QA"))

    def test_no_match(self):
        self.assertEqual(filter_choices(("A", "B"), "zzz"), ())

    def test_exact_selected_value_keeps_full_list(self):
        vals = ("Sales_Prod", "Sales_QA", "HR_QA")
        self.assertEqual(choices_for_typeahead(vals, "Sales_QA"), vals)

    def test_partial_still_filters(self):
        vals = ("Sales_Prod", "Sales_QA", "HR_QA")
        self.assertEqual(choices_for_typeahead(vals, "qa"), ("Sales_QA", "HR_QA"))


if __name__ == "__main__":
    unittest.main()
