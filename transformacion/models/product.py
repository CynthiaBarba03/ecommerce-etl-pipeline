"""
product.py — Esquema y reglas de negocio para la entidad Product (Productos)
============================================================================

PROPÓSITO:
    Define el esquema crudo, las reglas de limpieza de texto y el mapa de tipos
    para el catálogo de productos (products).

REGLAS DE NEGOCIO:
    1. 'name': Nombre del producto. Trim, eliminación de caracteres no permitidos,
       Title Case. Si es nulo o vacío -> 'UNKNOWN'.
    2. 'category': Categoría declarada en el producto. Title Case, nulos -> 'UNKNOWN'.
    3. 'brand': Marca comercial (ej: 'Nike', 'Lenovo', 'HP'). Title Case, nulos -> 'UNKNOWN'.
    4. 'description': Puede venir con etiquetas HTML (ej: '<p>...</p>'). Se limpian las
       etiquetas y se normalizan los espacios. Si es nula o vacía -> 'No description'.
    5. 'price': Precio de venta (DoubleType, no negativo).
    6. 'stock': Existencia disponible (IntegerType). Si viene 'unlimited' o texto no numérico -> null.
    7. 'created_at', 'updated_at': Fechas ISO (yyyy-MM-dd).
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
PRODUCT_SCHEMA = StructType([
    StructField("id",          IntegerType(), nullable=False),
    StructField("name",        StringType(),  nullable=True),
    StructField("category",    StringType(),  nullable=True),
    StructField("price",       StringType(),  nullable=True),
    StructField("stock",       StringType(),  nullable=True),
    StructField("brand",       StringType(),  nullable=True),
    StructField("description", StringType(),  nullable=True),
    StructField("created_at",  StringType(),  nullable=True),
    StructField("updated_at",  StringType(),  nullable=True),
])

# ============================================================
# REGLAS DE LIMPIEZA DE TEXTO POR COLUMNA
# ============================================================
PRODUCT_CLEANING_RULES = {
    "name": {
        "keep_chars":       "'-&/().",
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
    "category": {
        "keep_chars":       "'-&",
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
    "brand": {
        "keep_chars":       "'-.",
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
    "description": {
        "keep_chars":       "'-.,&/()",
        "null_replacement": "No description",
        "case_mode":        "none",
    },
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
PRODUCT_TYPE_MAP = {
    "id":    "int",
    "price": "double",
    "stock": "int",
}

PRODUCT_DATE_COLS = ["created_at", "updated_at"]
