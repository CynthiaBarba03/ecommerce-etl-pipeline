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

from transformacion.utils.text_utils import (
    strip_and_normalize_spaces,
    normalize_case,
    normalize_to_null,
    remove_special_chars,
)
from transformacion.utils.validation_utils import is_valid_email, is_valid_phone
from transformacion.utils.date_utils import format_date_to_iso, add_audit_timestamps
from transformacion.utils.cast_utils import cast_schema
from transformacion.cleaning.country_normalizer import normalize_country
from transformacion.models.customer import CLEANING_RULES, CUSTOMER_TYPE_MAP


def clean_text_column(
    df: DataFrame,
    column_name: str,
    keep_chars: str = "'-",
    replacement: str = "UNKNOWN",
    case_mode: str = "title",
) -> DataFrame:
    """
    Limpia una columna de texto aplicando las transformaciones estándar en orden:
        1. Quita caracteres especiales (configurables)
        2. Normaliza espacios (quita duplicados, trim)
        3. Reemplaza nulos/vacíos con el valor de reemplazo
        4. Capitaliza según el modo especificado

    ORDEN IMPORTA:
        Siempre en este orden:
        1. remove_special_chars → primero quitamos basura
        2. strip_and_normalize_spaces → luego limpiamos espacios
        3. normalize_to_null → DESPUÉS del trim para atrapar "   " (solo espacios)
        4. normalize_case → al final, cuando el texto ya está limpio

    PARÁMETROS:
        df:          DataFrame de PySpark
        column_name: Nombre de la columna a limpiar
        keep_chars:  Caracteres especiales que SÍ queremos conservar.
                     Por defecto: "'-" (apóstrofo y guión para nombres propios)
        replacement: Valor para nulos/vacíos (default: "UNKNOWN")
        case_mode:   "title" (John Smith), "upper" (JOHN), "lower" (john)

    RETORNA:
        DataFrame con columna nueva "{column_name}_clean" añadida

    EJEMPLO:
        df = clean_text_column(df, "name")
        # Crea columna "name_clean" con el nombre limpio y capitalizado
    """
    col = F.col(column_name)

    # 1. Quitar caracteres especiales (pero conservar los configurados)
    col = remove_special_chars(col, keep_chars=keep_chars)
    # 2. Normalizar espacios (trim + espacios internos)
    col = strip_and_normalize_spaces(col)
    # 3. Reemplazar vacíos/nulos → DESPUÉS del strip para atrapar "   "
    col = normalize_to_null(col, replacement=replacement)
    # 4. Capitalizar al final
    col = normalize_case(col, mode=case_mode)

    return df.withColumn(f"{column_name}_clean", col)


def clean_email_column(df: DataFrame, column_name: str = "email") -> DataFrame:
    """
    Limpia y valida el email del customer.

    DIFERENCIA CON clean_text_column:
        El email NO debe perder sus caracteres especiales (@, ., _, -)
        porque son parte válida del formato. Tampoco debe capitalizarse
        (los emails son case-insensitive pero la convención es minúsculas).

    PASOS:
        1. Quitar espacios
        2. Convertir a minúsculas
        3. Validar formato con regex
        4. Si es inválido → "UNKNOWN"

    RETORNA:
        DataFrame con columna nueva "email_clean"
    """
    col = F.col(column_name)

    # 1. Quitar espacios del borde
    cleaned = F.trim(col)
    # 2. Minúsculas (emails son case-insensitive, usamos siempre minúsculas)
    cleaned = F.lower(cleaned)

    # 3 + 4. Si no es válido → UNKNOWN
    cleaned = F.when(
        is_valid_email(cleaned),
        cleaned
    ).otherwise("UNKNOWN")

    return df.withColumn(f"{column_name}_clean", cleaned)


def clean_phone_column(df: DataFrame, column_name: str = "phone") -> DataFrame:
    """
    Limpia y valida el número de teléfono.

    ESTRATEGIA:
        Guardamos el teléfono "normalizado" (sin espacios ni guiones extra)
        si tiene entre 7 y 15 dígitos. Si no, lo marcamos como UNKNOWN.

        NO lo transformamos agresivamente porque los formatos de teléfono
        varían mucho por país y cliente. Es mejor conservar el original
        limpio que reescribir el formato.

    RETORNA:
        DataFrame con columna nueva "phone_clean"
    """
    col = F.col(column_name)

    # Normalizar espacios del borde
    cleaned = F.trim(col)

    # Validar y marcar inválidos
    cleaned = F.when(
        is_valid_phone(cleaned),
        cleaned
    ).otherwise("UNKNOWN")

    return df.withColumn(f"{column_name}_clean", cleaned)


def clean_date_columns(df: DataFrame, date_columns: list) -> DataFrame:
    """
    Estandariza varias columnas de fecha al formato ISO "yyyy-MM-dd".

    CREA columnas NUEVAS con sufijo "_clean" (NO reemplaza las originales):
        created_at        → valor original (tal como vino de la API)
        created_at_clean  → valor estandarizado "yyyy-MM-dd" (DateType)

    Esto permite comparar ANTES vs DESPUÉS lado a lado.

    CÓMO FORMATEA:
        Intenta parsear la fecha con ~20 formatos conocidos hasta encontrar
        el que coincida. Ejemplos:
            "01/05/2024"           → 2024-01-05
            "January 5, 2024"      → 2024-01-05
            "2024-01-05T14:30:00"  → 2024-01-05
            "20240105"             → 2024-01-05

        Si NO se puede parsear (basura, texto que no es fecha) → null.

    PARÁMETROS:
        df:           DataFrame de PySpark
        date_columns: Lista de nombres de columnas con fechas

    RETORNA:
        DataFrame con columnas "{col}_clean" añadidas (DateType)

    EJEMPLO:
        df = clean_date_columns(df, ["created_at", "updated_at", "registration_date"])
        # Crea: created_at_clean, updated_at_clean, registration_date_clean
    """
    from transformacion.utils.date_utils import parse_date

    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                f"{col_name}_clean",
                parse_date(F.col(col_name))
            )
        else:
            print(f"⚠️  Columna de fecha '{col_name}' no encontrada. Se omite.")
    return df


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
