"""
customer_cleaner.py — Limpieza específica de la entidad Customer
================================================================

PROPÓSITO:
    Este archivo SOLO contiene la lógica de negocio específica de customers.
    Las operaciones genéricas de texto, validación y fechas las delega
    a utils/, que son compartidas con todas las demás entidades.

COMPARACIÓN: ANTES vs AHORA

    ANTES (name_cleaner.py):
        def clean_name(df, column_name, replacement="UNKNOWN"):
            col = F.col(column_name)
            cleaned = F.regexp_replace(col, r"[^a-zA-Z\\s'-]", "")  ← lógica de texto aquí
            cleaned = F.trim(cleaned)                                  ← lógica de texto aquí
            cleaned = F.regexp_replace(cleaned, r"\\s+", " ")         ← lógica de texto aquí
            cleaned = F.when(...).otherwise(cleaned)
            cleaned = F.initcap(cleaned)
            df = df.withColumn(f"{column_name}_clean", cleaned)
            return df

    AHORA (customer_cleaner.py):
        def clean_text_column(df, column_name, replacement="UNKNOWN"):
            col = (
                remove_special_chars(F.col(column_name), keep_chars="'-")  ← utils
                .pipe(strip_and_normalize_spaces)                            ← utils
                .pipe(normalize_to_null, replacement)                       ← utils
                .pipe(normalize_case, "title")                              ← utils
            )
            df = df.withColumn(f"{column_name}_clean", col)
            return df

    La lógica de texto está en utils, aquí solo queda la COORDINACIÓN.

PRINCIPIO "LEGO":
    Piensa en utils/ como piezas de Lego individuales.
    customer_cleaner.py las ensambla en el orden correcto para customers.
    Mañana, product_cleaner.py usará las MISMAS piezas para products.
"""

from pyspark.sql import functions as F, DataFrame

from transformacion.cleaning.common import (
    clean_text_column,
    clean_date_columns,
    clean_email_column,
    clean_phone_column,
)
from transformacion.utils.date_utils import add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.cleaning.country_normalizer import normalize_country
from transformacion.models.customer import CLEANING_RULES, CUSTOMER_TYPE_MAP


def run_customer_cleaning(df: DataFrame, spark) -> DataFrame:
    """
    Función principal que orquesta TODA la limpieza de customers.

    FLUJO DE LIMPIEZA:
        1. Texto (nombre, ciudad, país) → clean_text_column
        2. Email → clean_email_column (lógica especial)
        3. Teléfono → clean_phone_column (validación de dígitos)
        4. País → country_normalizer (lookup ISO)
        5. Fechas → clean_date_columns (estandarizar a yyyy-MM-dd)
        6. Tipos → cast_schema (convertir de string al tipo correcto)
        7. Auditoría → add_audit_timestamps (cuándo fue procesado)

    POR QUÉ UNA SOLA FUNCIÓN PÚBLICA:
        El pipeline (pipeline_transformacion.py) solo necesita llamar
        a esta función. No necesita conocer los detalles internos de
        cómo se limpia cada columna. Eso es ENCAPSULAMIENTO.

    PARÁMETROS:
        df:    DataFrame de customers (datos crudos desde Bronze)
        spark: SparkSession (necesaria para el country normalizer)

    RETORNA:
        DataFrame limpio, listo para pasar a Silver Layer
    """
    # === PASO 1: Limpieza de columnas de texto ===
    # Leemos las reglas del modelo (customer.py) para saber qué columnas limpiar
    # y con qué configuración. Así el cleaner no tiene valores hardcodeados.
    for col_name, rules in CLEANING_RULES.items():
        if col_name in ("email", "phone", "country"):
            # Estas columnas tienen su propio tratamiento especial
            continue

        df = clean_text_column(
            df,
            column_name=col_name,
            keep_chars=rules.get("keep_chars", ""),
            replacement=rules.get("null_replacement", "UNKNOWN"),
            case_mode="title" if rules.get("capitalize") else "lower",
        )

    # === PASO 2: Email (tratamiento especial, no se capitaliza ni limpia agresivo) ===
    df = clean_email_column(df)

    # === PASO 3: Teléfono ===
    df = clean_phone_column(df)

    # === PASO 4: País (lookup ISO con pycountry) ===
    # El country_normalizer es el más complejo porque usa un join
    # con una tabla de referencia de países reconocidos internacionalmente
    df = normalize_country(df, spark)

    # === PASO 5: Estandarizar fechas a "yyyy-MM-dd" ===
    date_cols = ["created_at", "updated_at", "registration_date"]
    df = clean_date_columns(df, date_cols)

    # === PASO 6: Convertir tipos de datos ===
    # Después de limpiar, aplicamos el mapa de tipos del modelo
    df = cast_schema(df, CUSTOMER_TYPE_MAP)

    # === PASO 7: Agregar columnas de auditoría ===
    df = add_audit_timestamps(df)

    return df
