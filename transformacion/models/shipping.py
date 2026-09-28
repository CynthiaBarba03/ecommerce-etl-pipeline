"""
shipping.py — Esquema y reglas de negocio para la entidad Shipping (Envíos)
===========================================================================

PROPÓSITO:
    Define el esquema crudo, catálogos de transportistas y estados de entrega,
    y conversiones de tipos para el seguimiento logístico de envíos.

REGLAS DE NEGOCIO:
    1. 'carrier': Empresa transportista estandarizada ('FedEx', 'UPS', 'DHL', 'USPS',
       'Amazon Logistics', 'Local Courier'). Valores no reconocidos o vacíos -> 'UNKNOWN'.
    2. 'status': Estado del envío estandarizado a snake_case canónico:
       - 'pending', 'in_transit', 'delivered', 'delayed', 'lost', 'cancelled'.
       - No reconocidos o vacíos -> 'UNKNOWN'.
    3. 'tracking_number': Número de seguimiento oficial. Sin espacios, mayúsculas,
       si es nulo o vacío -> 'UNKNOWN'.
    4. 'cost': Costo de envío (DoubleType, no negativo).
    5. 'estimated_delivery', 'actual_delivery': Fechas de entrega previstas y reales (ISO yyyy-MM-dd).
    6. 'order_id': Clave foránea al pedido (IntegerType).
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
SHIPPING_SCHEMA = StructType([
    StructField("id",                 IntegerType(), nullable=False),
    StructField("order_id",           StringType(),  nullable=True),
    StructField("carrier",            StringType(),  nullable=True),
    StructField("tracking_number",    StringType(),  nullable=True),
    StructField("status",             StringType(),  nullable=True),
    StructField("estimated_delivery", StringType(),  nullable=True),
    StructField("actual_delivery",    StringType(),  nullable=True),
    StructField("cost",               StringType(),  nullable=True),
    StructField("created_at",         StringType(),  nullable=True),
    StructField("updated_at",         StringType(),  nullable=True),
])

# ============================================================
# CATÁLOGO CANÓNICO DE TRANSPORTISTAS
# ============================================================
SHIPPING_CARRIER_MAP = {
    "fedex": "FedEx",
    "ups": "UPS",
    "dhl": "DHL",
    "usps": "USPS",
    "amazon logistics": "Amazon Logistics",
    "amazon": "Amazon Logistics",
    "local courier": "Local Courier",
}

# ============================================================
# CATÁLOGO CANÓNICO DE ESTADOS DE ENVÍO
# ============================================================
SHIPPING_STATUS_MAP = {
    "pending": "pending",
    "in_transit": "in_transit",
    "in transit": "in_transit",
    "delivered": "delivered",
    "delayed": "delayed",
    "lost": "lost",
    "cancelled": "cancelled",
    "canceled": "cancelled",
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
SHIPPING_TYPE_MAP = {
    "id":       "int",
    "order_id": "int",
    "cost":     "double",
}

SHIPPING_DATE_COLS = ["estimated_delivery", "actual_delivery", "created_at", "updated_at"]
