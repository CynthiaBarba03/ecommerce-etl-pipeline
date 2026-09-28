"""
payment.py — Esquema y reglas de negocio para la entidad Payment (Pagos)
========================================================================

PROPÓSITO:
    Define el esquema crudo, catálogos canónicos de métodos y estados de pago,
    y el mapa de conversión de tipos para la tabla de transacciones de pago.

REGLAS DE NEGOCIO:
    1. 'method': Método de pago estandarizado a snake_case canónico:
       - 'credit_card', 'debit_card', 'paypal', 'cash', 'bank_transfer', 'crypto'.
       - Valores no reconocidos o vacíos -> 'UNKNOWN'.
    2. 'status': Estado del pago estandarizado:
       - 'completed', 'pending', 'failed', 'cancelled', 'refunded'.
       - Vacíos o nulos -> 'UNKNOWN'.
    3. 'transaction_id': Código de referencia único de la pasarela (ej: 'TXN-847388').
       Trim, mayúsculas, si es nulo o vacío -> 'UNKNOWN'.
    4. 'amount': Monto de la transacción (DoubleType, dos decimales).
    5. 'order_id': Clave foránea al pedido (IntegerType).
    6. 'paid_at': Fecha y hora del pago (ISO yyyy-MM-dd HH:mm:ss o yyyy-MM-dd).
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
PAYMENT_SCHEMA = StructType([
    StructField("id",             IntegerType(), nullable=False),
    StructField("order_id",       StringType(),  nullable=True),
    StructField("amount",         StringType(),  nullable=True),
    StructField("method",         StringType(),  nullable=True),
    StructField("status",         StringType(),  nullable=True),
    StructField("transaction_id", StringType(),  nullable=True),
    StructField("paid_at",        StringType(),  nullable=True),
    StructField("created_at",     StringType(),  nullable=True),
    StructField("updated_at",     StringType(),  nullable=True),
])

# ============================================================
# CATÁLOGO CANÓNICO DE MÉTODOS DE PAGO
# ============================================================
PAYMENT_METHOD_MAP = {
    "credit_card": "credit_card",
    "credit card": "credit_card",
    "credit": "credit_card",
    "debit_card": "debit_card",
    "debit card": "debit_card",
    "debit": "debit_card",
    "paypal": "paypal",
    "cash": "cash",
    "bank_transfer": "bank_transfer",
    "transfer": "bank_transfer",
    "crypto": "crypto",
    "bitcoin": "crypto",
}

# ============================================================
# CATÁLOGO CANÓNICO DE ESTADOS DE PAGO
# ============================================================
PAYMENT_STATUS_MAP = {
    "completed": "completed",
    "success": "completed",
    "paid": "completed",
    "pending": "pending",
    "failed": "failed",
    "declined": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "refunded": "refunded",
}

# ============================================================
# MAPA DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================
PAYMENT_TYPE_MAP = {
    "id":       "int",
    "order_id": "int",
    "amount":   "double",
}

PAYMENT_DATE_COLS = ["paid_at", "created_at", "updated_at"]
