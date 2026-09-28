"""
payment_cleaner.py — Limpieza específica de la entidad Payment (Pagos)
======================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de métodos de pago, estados,
    identificadores de transacción y tipos de datos numéricos a la tabla de pagos.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.common import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.payment import (
    PAYMENT_METHOD_MAP,
    PAYMENT_STATUS_MAP,
    PAYMENT_TYPE_MAP,
    PAYMENT_DATE_COLS,
)


def clean_payment_method(df: DataFrame, column_name: str = "method") -> DataFrame:
    """
    Estandariza los métodos de pago según PAYMENT_METHOD_MAP (ej: 'credit_card', 'paypal').
    Valores no reconocidos o vacíos se marcan como 'UNKNOWN'.
    """
    cleaned = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in PAYMENT_METHOD_MAP.items():
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


def clean_payment_status(df: DataFrame, column_name: str = "status") -> DataFrame:
    """
    Estandariza los estados de pago según PAYMENT_STATUS_MAP (ej: 'completed', 'pending').
    Valores no reconocidos o vacíos se marcan como 'UNKNOWN'.
    """
    cleaned = F.lower(F.trim(F.col(column_name)))

    mapping_expr = None
    for raw_val, canonical in PAYMENT_STATUS_MAP.items():
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


def run_payment_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de pagos.
    """
    # 1. Estandarización de método y estado
    df = clean_payment_method(df, "method")
    df = clean_payment_status(df, "status")

    # 2. Limpieza de transaction_id reutilizando clean_text_column
    df = clean_text_column(
        df,
        column_name="transaction_id",
        keep_chars="-",
        replacement="UNKNOWN",
        case_mode="upper",
    )

    # 3. Estandarización de fechas
    df = clean_date_columns(df, PAYMENT_DATE_COLS)

    # 4. Conversión de tipos seguros (id, order_id, amount)
    df = cast_schema(df, PAYMENT_TYPE_MAP)

    # 5. Auditoría
    df = add_audit_timestamps(df)

    return df
