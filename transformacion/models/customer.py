"""
customer.py — Esquema y reglas de negocio para la entidad Customer
==================================================================

PROPÓSITO:
    Este archivo define DOS cosas importantes:
    1. CUSTOMER_SCHEMA:  La estructura esperada del DataFrame (columnas y tipos)
    2. CLEANING_RULES:   Las reglas de cómo limpiar cada columna de texto
    3. CUSTOMER_TYPE_MAP: El mapa de conversión de tipos (string → tipo correcto)

¿POR QUÉ SEPARAR ESQUEMAS Y REGLAS DEL CÓDIGO DE LIMPIEZA?
    Imagina que mañana agregas una columna "middle_name" al dataset.
    Sin este archivo, tendrías que editar customer_cleaner.py para añadir
    la lógica de limpieza.
    CON este archivo, solo añades una línea aquí en CLEANING_RULES y
    el cleaner la procesa automáticamente. Eso es "Open/Closed Principle":
    abierto para extensión, cerrado para modificación.

VOCABULARIO IMPORTANTE:
    - StructType: La "plantilla" del DataFrame (como un CREATE TABLE en SQL)
    - StructField: Una columna (nombre + tipo + si puede ser null)
    - nullable=True: La columna PUEDE tener valores nulos
    - nullable=False: La columna NUNCA puede ser nula (falla si hay nulos)
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)

# ============================================================
# ESQUEMA DEL DATAFRAME CRUDO (tal como viene de la API/Bronze)
# ============================================================
# Cuando Spark lee el JSON, TODO viene como StringType.
# Este schema define lo que esperamos ANTES de limpiar.
CUSTOMER_SCHEMA = StructType([
    StructField("id",                IntegerType(), nullable=False),  # PK, nunca nulo
    StructField("name",              StringType(),  nullable=True),
    StructField("email",             StringType(),  nullable=True),
    StructField("phone",             StringType(),  nullable=True),
    StructField("city",              StringType(),  nullable=True),
    StructField("country",           StringType(),  nullable=True),
    StructField("created_at",        StringType(),  nullable=True),   # llega como string
    StructField("updated_at",        StringType(),  nullable=True),   # llega como string
    StructField("registration_date", StringType(),  nullable=True),   # llega como string
])

# ============================================================
# REGLAS DE LIMPIEZA POR COLUMNA
# ============================================================
# Formato: { "nombre_columna": { reglas... } }
# El customer_cleaner.py lee este diccionario para saber qué hacer
# con cada columna. Así el cleaner es genérico y este archivo
# contiene la especificidad del negocio.
CLEANING_RULES = {
    "name": {
        "keep_chars":       "'-",     # conservar apóstrofo y guión (O'Brian, Jean-Claude)
        "null_replacement": "UNKNOWN",
        "capitalize":       True,     # "john smith" → "John Smith"
    },
    "city": {
        "keep_chars":       "'-.",    # ciudades pueden tener puntos (St. Louis)
        "null_replacement": "UNKNOWN",
        "capitalize":       True,     # "new york" → "New York"
    },
    # "email" y "phone" y "country" tienen lógica especial y no van aquí
}

# ============================================================
# MAPA DE TIPOS DE DATOS FINALES (después de limpiar)
# ============================================================
# Después de limpiar los strings, convertimos al tipo correcto.
# Formato: { "nombre_columna": "tipo_destino" }
# Los tipos soportados están en cast_utils.py
CUSTOMER_TYPE_MAP = {
    "id":                "int",
    "created_at":        "date",    # yyyy-MM-dd → DateType
    "updated_at":        "date",
    "registration_date": "date",
}