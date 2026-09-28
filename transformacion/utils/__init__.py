# utils/ — Funciones reutilizables compartidas entre TODAS las entidades
#
# Estas funciones NO saben nada de "customers", "products" ni de ningún
# modelo de negocio. Solo saben hacer operaciones de texto, fechas y tipos.
# Eso las hace 100% reutilizables en cualquier parte del pipeline.

from .text_utils import (
    strip_and_normalize_spaces,
    normalize_case,
    normalize_to_null,
    remove_special_chars,
)
from .validation_utils import is_valid_email, is_valid_phone
from .date_utils import parse_date, format_date_to_iso
from .cast_utils import safe_cast_int, safe_cast_double, safe_cast_date

__all__ = [
    # text
    "strip_and_normalize_spaces",
    "normalize_case",
    "normalize_to_null",
    "remove_special_chars",
    # validation
    "is_valid_email",
    "is_valid_phone",
    # dates
    "parse_date",
    "format_date_to_iso",
    # casting
    "safe_cast_int",
    "safe_cast_double",
    "safe_cast_date",
]
