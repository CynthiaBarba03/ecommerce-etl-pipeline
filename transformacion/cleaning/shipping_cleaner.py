"""
shipping_cleaner.py — Limpieza específica de la entidad Shipping (Envíos)
=========================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de transportistas, estados de entrega,
    números de guía y costos a la tabla de envíos (shipping).
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.common import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.shipping import (
    SHIPPING_CARRIER_MAP,
    SHIPPING_STATUS_MAP,
    SHIPPING_TYPE_MAP,
    SHIPPING_DATE_COLS,
)


def clean_shipping_carrier(df: DataFrame, column_name: str = "carrier") -> DataFrame:
    """
    Estandariza los nombres de transportistas (ej: 'FedEx', 'UPS', 'DHL').
    """
    cleaned = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in SHIPPING_CARRIER_MAP.items():
        cond = (cleaned == raw_val)
        if mapping_expr is None:
            mapping_expr = F.when(cond, canonical)
        else:
            mapping_expr = mapping_expr.when(cond, canonical)

    if mapping_expr is None:
        mapping_expr = F.lit("UNKNOWN")
    else:
        mapping_expr = mapping_expr.otherwise("UNKNOWN")

    return df.withColumn(f"{column_name}_clean", mapping_expr)


def clean_shipping_status(df: DataFrame, column_name: str = "status") -> DataFrame:
    """
    Estandariza los estados logísticos (ej: 'in_transit', 'delivered', 'pending').
    """
    cleaned = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in SHIPPING_STATUS_MAP.items():
        cond = (cleaned == raw_val)
        if mapping_expr is None:
            mapping_expr = F.when(cond, canonical)
        else:
            mapping_expr = mapping_expr.when(cond, canonical)

    if mapping_expr is None:
        mapping_expr = F.lit("UNKNOWN")
    else:
        mapping_expr = mapping_expr.otherwise("UNKNOWN")

    return df.withColumn(f"{column_name}_clean", mapping_expr)


def run_shipping_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de envíos.
    """
    # 1. Estandarización de transportista y estado
    df = clean_shipping_carrier(df, "carrier")
    df = clean_shipping_status(df, "status")

    # 2. Número de seguimiento usando clean_text_column
    df = clean_text_column(
        df,
        column_name="tracking_number",
        keep_chars="-",
        replacement="UNKNOWN",
        case_mode="upper",
    )

    # 3. Estandarización de fechas (estimated_delivery, actual_delivery)
    df = clean_date_columns(df, SHIPPING_DATE_COLS)

    # 4. Conversión de tipos seguros (id, order_id, cost)
    df = cast_schema(df, SHIPPING_TYPE_MAP)

    # 5. Auditoría
    df = add_audit_timestamps(df)

    return df
