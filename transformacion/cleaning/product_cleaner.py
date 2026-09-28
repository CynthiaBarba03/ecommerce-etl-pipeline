"""
product_cleaner.py — Limpieza específica de la entidad Product (Productos)
==========================================================================

PROPÓSITO:
    Aplica las transformaciones de estandarización de nombres, categorías, marcas,
    descripciones (eliminando HTML) y tipos de datos a la tabla de productos.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.common import clean_text_column, clean_date_columns
from transformacion.utils.text_utils import strip_html_tags
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.models.product import (
    PRODUCT_CLEANING_RULES,
    PRODUCT_TYPE_MAP,
    PRODUCT_DATE_COLS,
)


def run_product_cleaning(df: DataFrame, spark=None) -> DataFrame:
    """
    Función orquestadora principal para la limpieza completa de productos.
    """
    # 1. Preprocesamiento: eliminar etiquetas HTML de la descripción si existen
    if "description" in df.columns:
        df = df.withColumn("description", strip_html_tags(F.col("description")))

    # 2. Preprocesamiento de stock: valores como 'unlimited' se convierten a null antes de castear
    if "stock" in df.columns:
        stock_clean = F.when(F.lower(F.trim(F.col("stock"))) == "unlimited", F.lit(None)).otherwise(F.col("stock"))
        df = df.withColumn("stock", stock_clean)

    # 3. Limpieza de texto ('name', 'category', 'brand', 'description') reutilizando clean_text_column
    for col_name, rules in PRODUCT_CLEANING_RULES.items():
        if col_name in df.columns:
            df = clean_text_column(
                df,
                column_name=col_name,
                keep_chars=rules.get("keep_chars", ""),
                replacement=rules.get("null_replacement", "UNKNOWN"),
                case_mode=rules.get("case_mode", "title"),
            )

    # 4. Estandarización de fechas
    df = clean_date_columns(df, PRODUCT_DATE_COLS)

    # 5. Conversión de tipos de datos seguros (id, price, stock)
    df = cast_schema(df, PRODUCT_TYPE_MAP)

    # 6. Auditoría
    df = add_audit_timestamps(df)

    return df
