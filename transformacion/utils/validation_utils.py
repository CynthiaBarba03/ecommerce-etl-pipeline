"""
validation_utils.py — Funciones de validación de formato para PySpark
======================================================================

PROPÓSITO:
    Valida que los datos tengan el formato correcto antes de procesarlos.
    NO modifica los datos, solo dice SÍ o NO a si el valor es válido.

    La diferencia con text_utils.py:
        - text_utils: LIMPIA el texto
        - validation_utils: VALIDA el formato

POR QUÉ SEPARAR VALIDACIÓN DE LIMPIEZA:
    Este patrón se llama "Single Responsibility Principle" (cada función
    hace UNA sola cosa). Es uno de los principios más importantes en
    ingeniería de software.

    Ejemplo de uso correcto:
        # Primero limpias
        cleaned = strip_and_normalize_spaces(F.col("email"))
        cleaned = F.lower(cleaned)

        # Luego validas
        valid = is_valid_email(cleaned)

        # Luego decides qué hacer con los inválidos
        final = F.when(valid, cleaned).otherwise("UNKNOWN")
"""

from pyspark.sql import functions as F
from pyspark.sql import Column


def is_valid_email(col: Column) -> Column:
    """
    Valida si el valor en la columna tiene formato de email correcto.

    RETORNA:
        Column de tipo BooleanType: True si es válido, False si no

    QUÉ VALIDA EL PATRÓN:
        ^[a-zA-Z0-9._%+-]+   → Parte local (antes del @):
                               letras, números, puntos, guiones bajos, etc.
        @                     → Obligatoriamente el símbolo @
        [a-zA-Z0-9.-]+        → Dominio (ej: "gmail", "hotmail")
        \\.                   → Un punto literal
        [a-zA-Z]{2,}$         → Extensión de al menos 2 letras (ej: "com", "mx", "io")

    EJEMPLOS:
        "user@gmail.com"    → True  ✅
        "HELLO@outlook.es"  → True  ✅  (mayúsculas son válidas)
        "nodomain"          → False ❌  (falta @)
        "falta@"            → False ❌  (falta el dominio)
        "@sinlocal.com"     → False ❌  (falta la parte antes del @)
        ""                  → False ❌  (vacío)
        None                → False ❌  (nulo)

    CÓMO USARLO EN EL PIPELINE:
        df = df.withColumn(
            "email_clean",
            F.when(is_valid_email(F.col("email")), F.lower(F.trim(F.col("email"))))
             .otherwise("UNKNOWN")
        )
    """
    # Este patrón regex cubre la gran mayoría de emails válidos del mundo real.
    # No es perfecto (el RFC 5321 es mucho más complejo), pero funciona para
    # datos de ecommerce. En producción, validar emails perfectamente es
    # prácticamente imposible sin enviar un email de verificación.
    email_pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

    return (
        col.isNotNull() &                    # No es nulo
        (F.trim(col) != "") &                # No es vacío
        col.rlike(email_pattern)             # Cumple el formato
    )


def is_valid_phone(col: Column) -> Column:
    """
    Valida si el valor tiene formato de número de teléfono.

    ESTRATEGIA:
        Los teléfonos son muy variados por país:
        - "+1 (555) 123-4567" (formato americano)
        - "+52 55 1234 5678"  (formato mexicano)
        - "555-1234"          (local sin código de país)

        En lugar de intentar validar todos los formatos del mundo,
        aplicamos una regla práctica:
        - El número debe tener entre 7 y 15 DÍGITOS (ignorando espacios,
          guiones, paréntesis y el símbolo +)
        - Esto es el estándar ITU-T E.164

    RETORNA:
        Column de tipo BooleanType: True si parece un teléfono válido

    EJEMPLOS:
        "+1 (555) 123-4567"  → True  ✅ (tiene 11 dígitos)
        "555-1234"           → True  ✅ (tiene 7 dígitos, mínimo)
        "123"                → False ❌ (muy pocos dígitos)
        "abc-defg"           → False ❌ (no tiene dígitos)
        ""                   → False ❌ (vacío)
    """
    # Primero extraemos solo los dígitos (eliminamos todo lo demás)
    # para contar cuántos dígitos reales tiene el número
    only_digits = F.regexp_replace(col, r"[^\d]", "")  # quita todo excepto dígitos

    digits_count = F.length(only_digits)

    return (
        col.isNotNull() &
        (F.trim(col) != "") &
        (digits_count >= 7) &   # Mínimo 7 dígitos (teléfonos locales)
        (digits_count <= 15)    # Máximo 15 dígitos (estándar internacional)
    )


def is_valid_date(col: Column, date_format: str = "yyyy-MM-dd") -> Column:
    """
    Valida si la columna contiene una fecha con el formato esperado.

    CÓMO FUNCIONA:
        Intentamos parsear el texto como fecha. Si el parseo da null,
        significa que el formato NO es correcto.

        Spark usa esta convención para los formatos de fecha:
            yyyy = año de 4 dígitos    (2024)
            MM   = mes de 2 dígitos    (01 para enero)
            dd   = día de 2 dígitos    (05 para el día 5)
            HH   = hora de 2 dígitos   (14 para las 2pm)
            mm   = minutos             (30)
            ss   = segundos            (00)

    PARÁMETROS:
        col:         Column de PySpark
        date_format: Formato esperado (por defecto "yyyy-MM-dd")

    EJEMPLOS DE FORMATOS:
        "yyyy-MM-dd"           → "2024-01-05"
        "dd/MM/yyyy"           → "05/01/2024"
        "yyyy-MM-dd HH:mm:ss"  → "2024-01-05 14:30:00"
        "MM/dd/yyyy"           → "01/05/2024" (formato americano)

    RETORNA:
        Column de tipo BooleanType: True si es una fecha válida
    """
    # try_to_date retorna null si no puede parsear → lo usamos como detector.
    # OJO: usamos _safe_to_date porque con ANSI mode (Databricks por defecto)
    # F.to_date LANZA DateTimeException en vez de devolver null.
    from transformacion.utils.date_utils import _safe_to_date
    parsed = _safe_to_date(col, date_format)
    return (
        col.isNotNull() &
        (F.trim(col) != "") &
        parsed.isNotNull()
    )
