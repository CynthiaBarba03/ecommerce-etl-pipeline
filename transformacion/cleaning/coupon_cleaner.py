"""
coupon_cleaner.py — Limpieza específica de la entidad Coupon (Cupones)
======================================================================

PROPÓSITO:
    Aplica las reglas de negocio y transformaciones de calidad de datos
    a la tabla de cupones de descuento (coupons), reutilizando las funciones
    compartidas de limpieza (clean_text_column, clean_date_columns).
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.customer_cleaner import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.coupon import (
    DISCOUNT_TYPE_MAP,
    COUPON_TYPE_MAP,
    COUPON_DATE_COLS,
)


def clean_discount_type(df: DataFrame, column_name: str = "discount_type") -> DataFrame:
    """
    Estandariza los tipos de descuento según el diccionario DISCOUNT_TYPE_MAP:
        - 'dollar', 'FIXED', '$' -> 'fixed'
        - 'PERCENT', 'percentage', '%' -> 'percentage'
        - 'bogo', 'buy_one_get_one' -> 'bogo'
        - 'free_shipping' -> 'free_shipping'
        - Valores vacíos o no válidos -> 'UNKNOWN'
    """
    col = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in DISCOUNT_TYPE_MAP.items():
        cond = (col == raw_val)
        if mapping_expr is None:
            mapping_expr = F.when(cond, canonical)
        else:
            mapping_expr = mapping_expr.when(cond, canonical)

    if mapping_expr is None:
        mapping_expr = F.lit("UNKNOWN")
    else:
        mapping_expr = mapping_expr.otherwise("UNKNOWN")

    return df.withColumn(f"{column_name}_clean", mapping_expr)


def run_coupon_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de cupones.
    """
    # 1. Limpieza del código de cupón usando la función ya existente
    df = clean_text_column(
        df,
        column_name="code",
        keep_chars="-",
        replacement="UNKNOWN",
        case_mode="upper",
    )

    # 2. Estandarización del tipo de descuento
    df = clean_discount_type(df, "discount_type")

    # 3. Estandarización de fechas usando la función ya existente
    df = clean_date_columns(df, COUPON_DATE_COLS)

    # 4. Conversión de tipos seguros (numéricos, enteros y booleanos)
    df = cast_schema(df, COUPON_TYPE_MAP)

    # 5. Agregar marcas de auditoría
    df = add_audit_timestamps(df)

    return df

