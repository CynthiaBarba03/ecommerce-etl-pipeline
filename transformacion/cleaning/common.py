"""Funciones compartidas por todos los cleaners (customers, coupons, ...).

Vivian en `customer_cleaner.py` y los demas cleaners las importaban desde ahi.
Eso invertia la dependencia (lo generico dependia de lo especifico).
Ahora viven aqui: `cleaning/common.py` solo depende de `utils/`.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.utils.date_utils import parse_date
from transformacion.utils.validation_utils import is_valid_email, is_valid_phone
from transformacion.utils.text_utils import (
    strip_and_normalize_spaces,
    normalize_case,
    normalize_to_null,
    remove_special_chars,
)


def clean_text_column(
    df: DataFrame,
    column_name: str,
    keep_chars: str = "'-",
    replacement: str = "UNKNOWN",
    case_mode: str = "title",
) -> DataFrame:
    """Limpia una columna de texto y crea `{column_name}_clean`.

    Orden: quitar especiales -> normalizar espacios -> nulos/vacios a
    `replacement` -> normalizar mayusculas (`title`/`upper`/`lower`).
    ``none`` conserva la redaccion despues de limpiar caracteres y espacios.
    """
    col = F.col(column_name)
    col = remove_special_chars(col, keep_chars=keep_chars)
    col = strip_and_normalize_spaces(col)
    col = normalize_to_null(col, replacement=replacement)
    if case_mode != "none":
        col = normalize_case(col, mode=case_mode)
    return df.withColumn(f"{column_name}_clean", col)


def clean_date_columns(df: DataFrame, date_columns: list) -> DataFrame:
    """Estandariza columnas de fecha a DateType en `{col}_clean`.

    Las originales se conservan como string crudo para comparar antes/despues.
    Columnas inexistentes se omiten con un aviso (no fallan).
    """
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(f"{col_name}_clean", parse_date(F.col(col_name)))
        else:
            print(f"Columna de fecha '{col_name}' no encontrada. Se omite.")
    return df


def clean_email_column(df: DataFrame, column_name: str = "email") -> DataFrame:
    """Normaliza y valida un email, conservando el valor original."""
    cleaned = F.lower(F.trim(F.col(column_name)))
    cleaned = F.when(is_valid_email(cleaned), cleaned).otherwise("UNKNOWN")
    return df.withColumn(f"{column_name}_clean", cleaned)


def clean_phone_column(df: DataFrame, column_name: str = "phone") -> DataFrame:
    """Normaliza y valida un telefono, conservando el valor original."""
    cleaned = F.trim(F.col(column_name))
    cleaned = F.when(is_valid_phone(cleaned), cleaned).otherwise("UNKNOWN")
    return df.withColumn(f"{column_name}_clean", cleaned)
