import re
from pyspark.sql import functions as F


def clean_email(df):
    """
    Limpia y valida la columna email.
    NO elimina caracteres especiales porque son parte válida del email (@, ., -, _).
    Reglas:
      1. Elimina espacios en blanco al inicio y al final
      2. Convierte a minúsculas
      3. Valida formato con regex
      4. Si no es un email válido o está vacío -> UNKNOWN
    """
    col = F.col("email")

    # Paso 1: Quitar espacios al inicio y al final
    cleaned = F.trim(col)

    # Paso 2: Convertir a minúsculas
    cleaned = F.lower(cleaned)

    # Paso 3: Si está vacío o nulo -> UNKNOWN
    cleaned = F.when(
        (cleaned.isNull()) | (cleaned == ""),
        "UNKNOWN"
    ).otherwise(cleaned)

    # Paso 4: Validar formato de email con regex
    # Patrón básico: algo@algo.dominio
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    cleaned = F.when(
        ~F.col("email").rlike(email_pattern) & (cleaned != "UNKNOWN"),
        "UNKNOWN"
    ).otherwise(cleaned)

    df = df.withColumn("email_clean", cleaned)

    return df