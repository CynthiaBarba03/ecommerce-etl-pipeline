"""
coupon.py — Esquema y reglas de negocio para la entidad Coupon (Cupones)
========================================================================

PROPÓSITO:
    Define el esquema crudo, las reglas de limpieza y el mapa de tipos de datos
    para la tabla de cupones de descuento (coupons).

REGLAS DE NEGOCIO:
    1. 'code': Código único del cupón. Debe estar en mayúsculas, sin espacios
       ni caracteres raros (ej: 'ZHY15', 'DUPLICATE'). Si es nulo o vacío -> 'UNKNOWN'.
    2. 'discount_type': Tipo de descuento ('percentage', 'fixed', 'bogo', 'free_shipping').
       Valores variados como 'dollar', 'PERCENT', 'Fixed' se estandarizan. Basura -> 'UNKNOWN'.
    3. 'discount_value': Valor del descuento (numérico con decimales, ej: 25.0, 10.5).
    4. 'min_order': Monto mínimo de compra para aplicar el cupón.
    5. 'max_uses', 'used_count': Contadores de uso (enteros).
    6. 'is_active': Estado booleano ('1'/'0', 'true'/'false', 'yes'/'no').
    7. 'expires_at': Fecha de expiración (formato ISO yyyy-MM-dd).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# ============================================================
# ESQUEMA DEL DATAFRAME CRUDO (Bronze)
# ============================================================
COUPON_SCHEMA = StructType([
    StructField("id",             IntegerType(), nullable=False),
    StructField("code",           StringType(),  nullable=True),
    StructField("discount_type",  StringType(),  nullable=True),
    StructField("discount_value", StringType(),  nullable=True),
    StructField("min_order",      StringType(),  nullable=True),
    StructField("max_uses",       StringType(),  nullable=True),
    StructField("used_count",     StringType(),  nullable=True),
    StructField("expires_at",     StringType(),  nullable=True),
    StructField("is_active",      StringType(),  nullable=True),
    StructField("created_at",     StringType(),  nullable=True),
    StructField("updated_at",     StringType(),  nullable=True),
])

# ============================================================
# NORMALIZACIÓN DE TIPOS DE DESCUENTO
# ============================================================
# Estandariza variaciones del catálogo de descuentos a valores canónicos:
DISCOUNT_TYPE_MAP = {
    "fixed": "fixed",
    "dollar": "fixed",
    "$": "fixed",
    "percentage": "percentage",
    "percent": "percentage",
    "%": "percentage",
    "bogo": "bogo",
    "buy_one_get_one": "bogo",
    "free_shipping": "free_shipping",
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
COUPON_TYPE_MAP = {
    "id":             "int",
    "discount_value": "double",
    "min_order":      "double",
    "max_uses":       "int",
    "used_count":     "int",
    "is_active":      "bool",
}

COUPON_DATE_COLS = ["expires_at", "created_at", "updated_at"]
