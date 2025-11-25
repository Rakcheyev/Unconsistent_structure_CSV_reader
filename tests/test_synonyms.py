"""Tests for synonym dictionary normalization."""
from __future__ import annotations

from core.normalization import SynonymDictionary


def test_normalize_matches_variant() -> None:
    dictionary = SynonymDictionary.from_mapping({"order_total": ["Order Total", "order-total"]})
    assert dictionary.normalize("order total") == "order_total"
    assert dictionary.normalize("unknown_col") == "unknown_col"
