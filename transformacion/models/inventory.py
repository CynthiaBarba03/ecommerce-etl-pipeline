"""
inventory.py — Esquema y reglas de negocio para la entidad Inventory (Inventario)
==================================================================================

PROPÓSITO:
    Define el esquema crudo, mapa de estandarización de estados y tipos de datos
    para el control de existencias en almacenes (inventory).

REGLAS DE NEGOCIO:
    1. 'product_id': Clave foránea al producto. Debe convertirse a int.
    2. 'warehouse': Nombre del almacén (ej: 'Main Hub', 'WH-Main', 'Warehouse A').
       Trim, eliminación de espacios múltiples, nulos/vacíos -> 'UNKNOWN'.
    3. 'quantity': Stock físico disponible (entero, no negativo).
    4. 'reorder_point': Umbral mínimo para reabastecimiento (entero).
    5. 'status': Estado del inventario estandarizado en snake_case:
       - 'in_stock'
       - 'low_stock'
       - 'out_of_stock'
       - 'discontinued'
       - 'pending'
       - Valores no reconocidos ('N/A', '', null) -> 'UNKNOWN'.
    6. 'last_restock': Fecha de último reabastecimiento (ISO yyyy-MM-dd).
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
INVENTORY_SCHEMA = StructType([
    StructField("id",            IntegerType(), nullable=False),
    StructField("product_id",    StringType(),  nullable=True),
    StructField("warehouse",     StringType(),  nullable=True),
    StructField("quantity",      StringType(),  nullable=True),
    StructField("reorder_point", StringType(),  nullable=True),
    StructField("last_restock",  StringType(),  nullable=True),
    StructField("status",        StringType(),  nullable=True),
    StructField("created_at",    StringType(),  nullable=True),
    StructField("updated_at",    StringType(),  nullable=True),
])

# ============================================================
# MAPA DE ESTADOS DE INVENTARIO
# ============================================================
INVENTORY_STATUS_MAP = {
    "in_stock": "in_stock",
    "in stock": "in_stock",
    "instock": "in_stock",
    "low_stock": "low_stock",
    "low stock": "low_stock",
    "out_of_stock": "out_of_stock",
    "out of stock": "out_of_stock",
    "discontinued": "discontinued",
    "pending": "pending",
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
INVENTORY_TYPE_MAP = {
    "id":            "int",
    "product_id":    "int",
    "quantity":      "int",
    "reorder_point": "int",
}

INVENTORY_DATE_COLS = ["last_restock", "created_at", "updated_at"]
