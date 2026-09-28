"""
category.py — Esquema y reglas de negocio para la entidad Category (Categorías)
================================================================================

PROPÓSITO:
    Define el esquema crudo, reglas de texto y mapa de tipos para la tabla de
    categorías de productos (categories).

REGLAS DE NEGOCIO:
    1. 'name': Nombre de la categoría. Limpieza de caracteres raros, trim,
       capitalizado en Title Case (ej: 'Food & Beverages', 'Books').
       Si es nulo o vacío -> 'UNKNOWN'.
    2. 'description': Descripción libre. Normalización de espacios.
       Si es nulo o vacío -> 'No description'.
    3. 'parent_id': Clave foránea autorreferencial (subcategorías). Entero o null.
    4. 'is_active': Booleano ('true'/'false', '1'/'0').
    5. 'created_at', 'updated_at': Fechas ISO (yyyy-MM-dd).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# ============================================================
# ESQUEMA DEL DATAFRAME CRUDO (Bronze)
# ============================================================
CATEGORY_SCHEMA = StructType([
    StructField("id",          IntegerType(), nullable=False),
    StructField("name",        StringType(),  nullable=True),
    StructField("parent_id",   StringType(),  nullable=True),
    StructField("description", StringType(),  nullable=True),
    StructField("is_active",   StringType(),  nullable=True),
    StructField("created_at",  StringType(),  nullable=True),
    StructField("updated_at",  StringType(),  nullable=True),
])

# ============================================================
# REGLAS DE LIMPIEZA DE TEXTO
# ============================================================
CATEGORY_CLEANING_RULES = {
    "name": {
        "keep_chars":       "'-&",     # 'Food & Beverages', 'Men's Wear'
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
    "description": {
        "keep_chars":       "'-.,&/()", # puntuación común en descripciones
        "null_replacement": "No description",
        "case_mode":        "none",     # mantener puntuación y redacción original
    }
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
CATEGORY_TYPE_MAP = {
    "id":        "int",
    "parent_id": "int",
    "is_active": "bool",
}

CATEGORY_DATE_COLS = ["created_at", "updated_at"]
