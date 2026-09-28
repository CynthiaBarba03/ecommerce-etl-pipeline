"""
category_cleaner.py — Limpieza específica de la entidad Category (Categorías)
=============================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de nombres, descripciones,
    tipos de datos y fechas a la tabla de categorías de productos (categories),
    reutilizando las funciones genéricas de limpieza ya definidas.
"""

from pyspark.sql import DataFrame

from transformacion.cleaning.common import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.category import (
    CATEGORY_CLEANING_RULES,
    CATEGORY_TYPE_MAP,
    CATEGORY_DATE_COLS,
)


def run_category_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de categories.

    Reutiliza directamente:
        - clean_text_column: para 'name' y 'description' con sus reglas
        - clean_date_columns: para parsear fechas
        - cast_schema: para convertir tipos
        - add_audit_timestamps: para trazabilidad
    """
    # 1. Limpieza de texto aplicando las reglas ya definidas en el modelo
    for col_name, rules in CATEGORY_CLEANING_RULES.items():
        if col_name in df.columns:
            df = clean_text_column(
                df,
                column_name=col_name,
                keep_chars=rules.get("keep_chars", ""),
                replacement=rules.get("null_replacement", "UNKNOWN"),
                case_mode=rules.get("case_mode", "title"),
            )

    # 2. Fechas (created_at, updated_at)
    df = clean_date_columns(df, CATEGORY_DATE_COLS)

    # 3. Conversión de tipos (id a int, is_active a bool)
    df = cast_schema(df, CATEGORY_TYPE_MAP)

    # 4. Auditoría
    df = add_audit_timestamps(df)

    return df

