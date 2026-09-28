"""
inventory_cleaner.py — Limpieza específica de la entidad Inventory (Inventario)
================================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de nombres de almacén,
    estados de stock, cantidades numéricas y fechas de reposición a la tabla
    de inventario (inventory), reutilizando las funciones de limpieza ya existentes.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.customer_cleaner import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.inventory import (
    INVENTORY_STATUS_MAP,
    INVENTORY_TYPE_MAP,
    INVENTORY_DATE_COLS,
)


def clean_inventory_status(df: DataFrame, column_name: str = "status") -> DataFrame:
    """
    Estandariza los estados de inventario a snake_case canónico:
        - 'in_stock', 'in stock' -> 'in_stock'
        - 'low_stock', 'low stock' -> 'low_stock'
        - 'out_of_stock', 'out of stock' -> 'out_of_stock'
        - 'discontinued' -> 'discontinued'
        - 'pending' -> 'pending'
        - 'N/A', '', nulos -> 'UNKNOWN'
    """
    cleaned = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in INVENTORY_STATUS_MAP.items():
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


def run_inventory_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de inventario.
    """
    # 1. Limpieza de texto de almacén usando la función ya existente
    df = clean_text_column(
        df,
        column_name="warehouse",
        keep_chars="-",
        replacement="UNKNOWN",
        case_mode="title",
    )

    # 2. Estandarización de estado categórico
    df = clean_inventory_status(df, "status")

    # 3. Fechas usando la función ya existente
    df = clean_date_columns(df, INVENTORY_DATE_COLS)

    # 4. Conversión de tipos numéricos
    df = cast_schema(df, INVENTORY_TYPE_MAP)

    # 5. Auditoría
    df = add_audit_timestamps(df)

    return df

