"""
review_cleaner.py — Limpieza específica de la entidad Review (Reseñas)
======================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de comentarios, títulos,
    puntuaciones (ratings) y marcas temporales a la tabla de reseñas.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.common import clean_text_column, clean_date_columns
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.review import (
    REVIEW_CLEANING_RULES,
    REVIEW_TYPE_MAP,
    REVIEW_DATE_COLS,
)


def run_review_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de reseñas.
    """
    # 1. Limpieza de texto de título y comentario reutilizando clean_text_column
    for col_name, rules in REVIEW_CLEANING_RULES.items():
        if col_name in df.columns:
            df = clean_text_column(
                df,
                column_name=col_name,
                keep_chars=rules.get("keep_chars", ""),
                replacement=rules.get("null_replacement", "No title"),
                case_mode=rules.get("case_mode", "none"),
            )

    # 2. Estandarización de fechas
    df = clean_date_columns(df, REVIEW_DATE_COLS)

    # 3. Conversión de tipos seguros (id, product_id, customer_id, rating, verified_purchase)
    df = cast_schema(df, REVIEW_TYPE_MAP)

    # 4. Auditoría
    df = add_audit_timestamps(df)

    return df
