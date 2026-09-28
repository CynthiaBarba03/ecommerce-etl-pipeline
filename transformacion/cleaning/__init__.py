# cleaning/ — Limpiadores por entidad
#
# Cada archivo en esta carpeta es responsable de la limpieza de UNA entidad.
# Todos usan las funciones de utils/ para las operaciones básicas.

from .common import (
    clean_text_column,
    clean_date_columns,
    clean_email_column,
    clean_phone_column,
)
from .customer_cleaner import run_customer_cleaning
from .country_normalizer import normalize_country
from .coupon_cleaner import run_coupon_cleaning
from .category_cleaner import run_category_cleaning
from .inventory_cleaner import run_inventory_cleaning
from .supplier_cleaner import run_supplier_cleaning

__all__ = [
    "run_customer_cleaning",
    "clean_text_column",
    "clean_date_columns",
    "clean_email_column",
    "clean_phone_column",
    "normalize_country",
    "run_coupon_cleaning",
    "run_category_cleaning",
    "run_inventory_cleaning",
    "run_supplier_cleaning",
]