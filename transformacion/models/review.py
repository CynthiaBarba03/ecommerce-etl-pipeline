"""
review.py — Esquema y reglas de negocio para la entidad Review (Reseñas)
========================================================================

PROPÓSITO:
    Define el esquema crudo, reglas de texto y mapa de tipos para las valoraciones
    y comentarios de clientes sobre productos (reviews).

REGLAS DE NEGOCIO:
    1. 'rating': Puntuación entera (normalmente entre 1 y 5 estrellas).
    2. 'title': Título o resumen de la reseña. Si es nulo o vacío -> 'No title'.
    3. 'comment': Comentario detallado. Normalización de espacios. Si es nulo -> 'No comment'.
    4. 'verified_purchase': Booleano ('true'/'false', '1'/'0').
    5. 'product_id', 'customer_id': Claves foráneas (IntegerType).
    6. 'created_at', 'updated_at': Fechas ISO (yyyy-MM-dd).
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
REVIEW_SCHEMA = StructType([
    StructField("id",                IntegerType(), nullable=False),
    StructField("product_id",        StringType(),  nullable=True),
    StructField("customer_id",       StringType(),  nullable=True),
    StructField("rating",            StringType(),  nullable=True),
    StructField("title",             StringType(),  nullable=True),
    StructField("comment",           StringType(),  nullable=True),
    StructField("verified_purchase", StringType(),  nullable=True),
    StructField("created_at",        StringType(),  nullable=True),
    StructField("updated_at",        StringType(),  nullable=True),
])

# ============================================================
# REGLAS DE LIMPIEZA DE TEXTO
# ============================================================
REVIEW_CLEANING_RULES = {
    "title": {
        "keep_chars":       "'-.,!?()",
        "null_replacement": "No title",
        "case_mode":        "title",
    },
    "comment": {
        "keep_chars":       "'-.,!?()",
        "null_replacement": "No comment",
        "case_mode":        "none",
    },
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
REVIEW_TYPE_MAP = {
    "id":                "int",
    "product_id":        "int",
    "customer_id":       "int",
    "rating":            "int",
    "verified_purchase": "bool",
}

REVIEW_DATE_COLS = ["created_at", "updated_at"]
