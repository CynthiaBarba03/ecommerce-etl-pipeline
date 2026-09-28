"""
test_models_and_cleaners.py — Pruebas unitarias para modelos y reglas de coupons, categories e inventory
"""

from transformacion.models.coupon import DISCOUNT_TYPE_MAP, COUPON_TYPE_MAP
from transformacion.models.category import CATEGORY_CLEANING_RULES, CATEGORY_TYPE_MAP
from transformacion.models.inventory import INVENTORY_STATUS_MAP, INVENTORY_TYPE_MAP


def test_discount_type_map():
    assert DISCOUNT_TYPE_MAP["dollar"] == "fixed"
    assert DISCOUNT_TYPE_MAP["fixed"] == "fixed"
    assert DISCOUNT_TYPE_MAP["percent"] == "percentage"
    assert DISCOUNT_TYPE_MAP["percentage"] == "percentage"
    assert DISCOUNT_TYPE_MAP["bogo"] == "bogo"


def test_category_rules():
    assert "name" in CATEGORY_CLEANING_RULES
    assert CATEGORY_CLEANING_RULES["name"]["case_mode"] == "title"
    assert CATEGORY_TYPE_MAP["is_active"] == "bool"


def test_inventory_status_map():
    assert INVENTORY_STATUS_MAP["in stock"] == "in_stock"
    assert INVENTORY_STATUS_MAP["low stock"] == "low_stock"
    assert INVENTORY_STATUS_MAP["out of stock"] == "out_of_stock"
    assert INVENTORY_STATUS_MAP["discontinued"] == "discontinued"
    assert INVENTORY_STATUS_MAP["pending"] == "pending"
