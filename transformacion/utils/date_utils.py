"""
date_utils.py — Funciones de fechas para PySpark
=================================================

PROPÓSITO:
    En los datos crudos de un ecommerce, las fechas pueden venir
    en MUCHOS formatos diferentes dependiendo de la fuente:
        - "2024-01-05"           (ISO estándar)
        - "01/05/2024"           (formato americano mes/día/año)
        - "05/01/2024"           (formato europeo día/mes/año)
        - "2024-1-5"             (sin ceros, no estándar)
        - "January 5, 2024"      (formato texto)
        - "2024-01-05T14:30:00"  (formato ISO con hora)

    Este archivo estandariza TODO al formato ISO 8601: "yyyy-MM-dd"
    Ese es el formato estándar internacional para fechas.

POR QUÉ ISO 8601 (yyyy-MM-dd):
    - Es el estándar internacional (IEEE, ISO, W3C)
    - Ordena cronológicamente si ordenas alfabéticamente (2024 > 2023)
    - No hay ambigüedad: 01/05 puede ser enero-5 o mayo-1 según el país
    - Todos los sistemas de bases de datos lo entienden nativamente

NOTA PARA APRENDER:
    En PySpark, las fechas tienen DOS representaciones:
    1. StringType: "2024-01-05" (texto)
    2. DateType:   2024-01-05   (tipo fecha real, sin comillas)

    Cuando quieres hacer cálculos (diferencia entre fechas, etc.)
    necesitas DateType. Para guardar en texto o mostrar, StringType.
"""

from pyspark.sql import functions as F
from pyspark.sql import Column
from typing import List


def _safe_to_date(col: Column, date_format: str) -> Column:
    """
    to_date SEGURO para entornos con ANSI mode activado (Databricks lo activa
    por defecto desde DBR 14+).

    ¿POR QUÉ EXISTE ESTA FUNCIÓN?
        Con ANSI mode ON, F.to_date() LANZA DateTimeException cuando el valor
        no coincide con el formato, en vez de devolver null.

        Ejemplo del bug que resuelve:
            to_date("2022-07-31", "MM/dd/yyyy")
            → ANSI OFF:  null            (falla silenciosamente, bien)
            → ANSI ON:   DateTimeException (¡explota!)

        try_to_date (Spark 3.5+) siempre devuelve null si no puede parsear.

    RETORNA:
        Column DateType, null si no se puede parsear (NUNCA lanza excepción)
    """
    if hasattr(F, "try_to_date"):
        # Spark 3.5+ / Databricks DBR 14+: versión segura
        return F.try_to_date(col, date_format)
    # Fallback para versiones antiguas de Spark
    return F.to_date(col, date_format)


# Formatos que sabemos que pueden venir en los datos crudos del ecommerce.
# PySpark intentará cada uno en orden hasta que alguno funcione.
KNOWN_DATE_FORMATS: List[str] = [
    "yyyy-MM-dd",           # Estándar ISO: 2024-01-05
    "yyyy-MM-dd HH:mm:ss",  # ISO con hora: 2024-01-05 14:30:00
    "yyyy-MM-dd'T'HH:mm:ss",# ISO 8601 con T: 2024-01-05T14:30:00
    "yyyy-MM-dd HH:mm:ss.SSS",  # ISO con milisegundos: 2024-01-05 14:30:00.123
    "MM/dd/yyyy",           # Americano: 01/05/2024
    "dd/MM/yyyy",           # Europeo: 05/01/2024
    "MM-dd-yyyy",           # Americano con guión: 01-05-2024
    "dd-MM-yyyy",           # Europeo con guión: 05-01-2024
    "yyyy/MM/dd",           # Asiático: 2024/01/05
    "d/M/yyyy",             # Sin ceros: 5/1/2024
    "M/d/yyyy",             # Sin ceros americano: 1/5/2024
    # === Formatos adicionales (más cobertura, menos nulls) ===
    "yyyy.MM.dd",           # Con punto: 2024.01.05
    "dd.MM.yyyy",           # Europeo con punto: 05.01.2024
    "MM.dd.yyyy",           # Americano con punto: 01.05.2024
    "dd/MM/yyyy HH:mm:ss",  # Europeo con hora: 05/01/2024 14:30:00
    "MM/dd/yyyy HH:mm:ss",  # Americano con hora: 01/05/2024 14:30:00
    "yyyyMMdd",             # Compacto: 20240105
    "MMMM d, yyyy",         # Mes en inglés: January 5, 2024
    "MMM d, yyyy",          # Mes corto en inglés: Jan 5, 2024
    "d MMMM yyyy",          # 5 January 2024
    "d MMM yyyy",           # 5 Jan 2024
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",  # ISO con Z: 2024-01-05T14:30:00.000Z
]

# El formato de salida estándar que usamos siempre
ISO_DATE_FORMAT = "yyyy-MM-dd"


def parse_date(col: Column, input_format: str = None) -> Column:
    """
    Convierte un texto con fecha a tipo DateType de Spark.

    DIFERENCIA ENTRE StringType y DateType:
        StringType: "2024-01-05"  (es solo texto, Spark no sabe que es fecha)
        DateType:   2024-01-05    (Spark sabe que es fecha, permite cálculos)

    CON DateType PUEDES HACER:
        - Calcular diferencia entre fechas: datediff(fecha1, fecha2)
        - Extraer el año: year(fecha), el mes: month(fecha)
        - Ordenar cronológicamente correctamente

    PARÁMETROS:
        col:          Column con el texto de la fecha
        input_format: Si sabes el formato exacto, ponlo aquí.
                      Si no lo sabes, deja None y probaremos los formatos
                      conocidos (KNOWN_DATE_FORMATS) en orden.

    RETORNA:
        Column de tipo DateType. Si no se puede parsear → null

    EJEMPLOS:
        parse_date(F.col("created_at"))
            → DateType, null si no se pudo parsear

        parse_date(F.col("created_at"), "dd/MM/yyyy")
            → Parsea específicamente ese formato
    """
    if input_format:
        # Si nos dijeron el formato exacto, lo usamos directamente (versión segura)
        return _safe_to_date(col, input_format)

    # Si no conocemos el formato, probamos todos los formatos conocidos
    # Construimos una cadena de CASE WHEN: si el primero falla, intenta el siguiente
    # Esto es como: "intenta formato 1, si es null intenta formato 2, etc."
    result = F.lit(None).cast("date")  # empezamos con null

    # Iteramos de atrás hacia adelante para que el primero de la lista tenga prioridad
    for fmt in reversed(KNOWN_DATE_FORMATS):
        parsed = _safe_to_date(col, fmt)  # ← seguro con ANSI mode
        # Si el intento anterior dio null, usamos este; si no, mantenemos el anterior
        result = F.when(result.isNull(), parsed).otherwise(result)

    return result


def format_date_to_iso(col: Column, input_format: str = None) -> Column:
    """
    Convierte una fecha de CUALQUIER formato al estándar ISO "yyyy-MM-dd" (como STRING).

    CUÁNDO USAR format_date_to_iso vs parse_date:
        - parse_date → cuando quieres DateType para hacer cálculos con la fecha
        - format_date_to_iso → cuando quieres un string "yyyy-MM-dd" para guardar
                               o mostrar en texto estandarizado

    PARÁMETROS:
        col:          Column con el texto de la fecha (en cualquier formato)
        input_format: El formato original. Si es None, se auto-detecta.

    RETORNA:
        Column de tipo StringType con el texto "yyyy-MM-dd".
        Si no se puede parsear → null

    EJEMPLOS:
        "01/05/2024" → "2024-01-05"
        "5 ene 2024" → null  (formato no reconocido)
        "2024-01-05T14:30:00" → "2024-01-05"  (corta la hora)
    """
    # Primero convertimos a DateType, luego formateamos como string ISO
    parsed_date = parse_date(col, input_format)
    return F.date_format(parsed_date, ISO_DATE_FORMAT)


def add_audit_timestamps(df, timestamp_col: str = "ingestion_timestamp"):
    """
    Agrega columnas de auditoría con la fecha y hora de procesamiento.

    PARA QUÉ SIRVEN LAS COLUMNAS DE AUDITORÍA:
        En producción SIEMPRE quieres saber:
        - ¿Cuándo fue procesado este registro?
        - ¿De qué lote de datos viene?

        Estas columnas son esenciales para:
        - Debug: "¿cuándo empezó a fallar esto?"
        - Reprocess: "necesito reprocesar lo de ayer"
        - Auditoría legal: "prueba de cuándo recibimos estos datos"

    QUÉ AGREGA:
        - {timestamp_col}: Timestamp exacto de procesamiento (con zona horaria UTC)
        - ingestion_date:  Solo la fecha de hoy "yyyy-MM-dd"

    PARÁMETROS:
        df:            DataFrame de PySpark
        timestamp_col: Nombre de la columna de timestamp (por defecto: "ingestion_timestamp")

    RETORNA:
        DataFrame con las columnas de auditoría añadidas
    """
    return df.withColumn(
        timestamp_col,
        F.current_timestamp()  # Fecha y hora actual del servidor (UTC en cloud)
    ).withColumn(
        "ingestion_date",
        F.current_date()       # Solo la fecha de hoy "yyyy-MM-dd"
    )
