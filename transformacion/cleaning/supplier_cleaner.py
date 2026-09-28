"""Limpieza de la entidad Supplier."""

from pyspark.sql import DataFrame

from transformacion.cleaning.common import (
    clean_date_columns,
    clean_email_column,
    clean_phone_column,
    clean_text_column,
)
from transformacion.cleaning.country_normalizer import normalize_country
from transformacion.models.supplier import (
    SUPPLIER_CLEANING_RULES,
    SUPPLIER_DATE_COLS,
    SUPPLIER_TYPE_MAP,
)
from transformacion.utils.cast_utils import cast_schema
from transformacion.utils.date_utils import add_audit_timestamps


def run_supplier_cleaning(df: DataFrame, spark) -> DataFrame:
    """Aplica reglas de texto, contacto, pais, fechas, tipos y auditoria."""
    for column_name, rules in SUPPLIER_CLEANING_RULES.items():
        if column_name in df.columns:
            df = clean_text_column(
                df,
                column_name,
                keep_chars=rules.get("keep_chars", ""),
                replacement=rules.get("null_replacement", "UNKNOWN"),
                case_mode=rules.get("case_mode", "title"),
            )

    if "email" in df.columns:
        df = clean_email_column(df)
    if "phone" in df.columns:
        df = clean_phone_column(df)
    if "country" in df.columns:
        df = normalize_country(df, spark)

    df = clean_date_columns(df, SUPPLIER_DATE_COLS)
    df = cast_schema(df, SUPPLIER_TYPE_MAP)
    return add_audit_timestamps(df)