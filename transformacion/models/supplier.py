"""
supplier.py — Esquema y reglas de negocio para la entidad Supplier (Proveedores)
================================================================================

PROPÓSITO:
    Define el esquema crudo, reglas de texto y mapa de tipos para los proveedores
    y suministradores mayoristas del ecommerce.

REGLAS DE NEGOCIO:
    1. 'name': Razón social del proveedor. Title Case, caracteres especiales
       permitidos (ej: 'TechCorp Inc', 'ProSupplies & Co.'). Vacíos -> 'UNKNOWN'.
    2. 'contact_name': Nombre del contacto principal. Title Case. Vacíos -> 'UNKNOWN'.
    3. 'email': Validación y normalización con is_valid_email() (clean_email_column).
    4. 'phone': Validación y normalización con is_valid_phone() (clean_phone_column).
    5. 'country': Normalización de país según estándar ISO con country_normalizer.
    6. 'rating': Puntuación comercial (DoubleType). Valores negativos -> null.
    7. 'is_active': Booleano ('true'/'false', '1'/'0').
    8. 'created_at', 'updated_at': Fechas ISO (yyyy-MM-dd).
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
SUPPLIER_SCHEMA = StructType([
    StructField("id",           IntegerType(), nullable=False),
    StructField("name",         StringType(),  nullable=True),
    StructField("contact_name", StringType(),  nullable=True),
    StructField("email",        StringType(),  nullable=True),
    StructField("phone",        StringType(),  nullable=True),
    StructField("country",      StringType(),  nullable=True),
    StructField("rating",       StringType(),  nullable=True),
    StructField("is_active",    StringType(),  nullable=True),
    StructField("created_at",   StringType(),  nullable=True),
    StructField("updated_at",   StringType(),  nullable=True),
])

# ============================================================
# REGLAS DE LIMPIEZA DE TEXTO
# ============================================================
SUPPLIER_CLEANING_RULES = {
    "name": {
        "keep_chars":       "'-.&",
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
    "contact_name": {
        "keep_chars":       "'-",
        "null_replacement": "UNKNOWN",
        "case_mode":        "title",
    },
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
SUPPLIER_TYPE_MAP = {
    "id":        "int",
    "rating":    "double",
    "is_active": "bool",
}

SUPPLIER_DATE_COLS = ["created_at", "updated_at"]
