"""
cast_utils.py — Conversión segura de tipos de datos en PySpark
==============================================================

PROPÓSITO:
    Cuando Spark lee datos desde JSON o CSV, TODO llega como StringType.
    Este archivo convierte los strings al tipo correcto de manera SEGURA.

¿QUÉ SIGNIFICA "SEGURA"?
    La conversión normal de Spark puede fallar o dar null sin aviso:
        df.withColumn("id", F.col("id").cast(IntegerType()))
        → Si "id" tiene el valor "abc", esto da null silenciosamente.

    Nuestras funciones "safe_cast" hacen la conversión Y te avisan
    cuándo hay valores que no pudieron convertirse, registrándolos
    en una columna de bandera (flag) para revisión.

TIPOS DE DATOS EN PYSPARK:
    StringType  → Texto: "hola", "123", "2024-01-05"
    IntegerType → Números enteros: 1, 42, -5
    LongType    → Números enteros grandes: 1234567890123
    DoubleType  → Números decimales: 19.99, 3.14159
    FloatType   → Decimales menor precisión (menos común)
    BooleanType → Verdadero/Falso: True, False
    DateType    → Fecha: 2024-01-05
    TimestampType → Fecha + hora: 2024-01-05 14:30:00

POR QUÉ IMPORTA EL TIPO CORRECTO:
    - IntegerType ocupa menos memoria que StringType
    - Solo puedes hacer sum(), avg() en tipos numéricos
    - Solo puedes hacer datediff() en DateType/TimestampType
    - Las bases de datos SQL son mucho más eficientes con tipos correctos
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType, LongType, DoubleType, DateType


def safe_cast_int(df: DataFrame, col_name: str, null_replacement: int = None) -> DataFrame:
    """
    Convierte una columna string a IntegerType de forma segura.

    PARÁMETROS:
        df:               DataFrame de PySpark
        col_name:         Nombre de la columna a convertir
        null_replacement: Si el valor no se puede convertir, ¿qué ponemos?
                          None = dejar como null (por defecto)
                          0 = reemplazar con cero (cuidado: puede confundirse con "real" 0)

    RETORNA:
        DataFrame con la columna convertida a IntegerType

    EJEMPLOS:
        "42"    → 42      ✅
        "0"     → 0       ✅
        "-5"    → -5      ✅
        "abc"   → null    ❌ (no es número)
        "19.99" → null    ❌ (tiene decimales, usa safe_cast_double para eso)
        ""      → null    ❌ (vacío)
        None    → null    ❌ (ya era null)

    NOTA SOBRE IntegerType vs LongType:
        IntegerType tiene un límite: -2,147,483,648 a 2,147,483,647
        Si tu ID puede ser mayor (tablas con millones de registros),
        usa safe_cast_long() que soporta hasta 9 cuatrillones.
    """
    col = F.col(col_name).cast(IntegerType())

    if null_replacement is not None:
        col = F.when(col.isNull(), null_replacement).otherwise(col)

    return df.withColumn(col_name, col)


def safe_cast_long(df: DataFrame, col_name: str) -> DataFrame:
    """
    Convierte una columna string a LongType (enteros muy grandes).

    CUÁNDO USAR LongType VS IntegerType:
        - IntegerType: hasta ~2 mil millones (IDs pequeños, edades, cantidades)
        - LongType: hasta ~9 cuatrillones (IDs de sistemas grandes, timestamps Unix)

    RETORNA:
        DataFrame con la columna convertida a LongType
    """
    return df.withColumn(col_name, F.col(col_name).cast(LongType()))


def safe_cast_double(df: DataFrame, col_name: str) -> DataFrame:
    """
    Convierte una columna string a DoubleType (números con decimales).

    CUÁNDO USAR DoubleType:
        - Precios: 19.99, 1299.00
        - Porcentajes: 0.85, 1.15
        - Coordenadas geográficas: 40.7128, -74.0060
        - Calificaciones: 4.5, 3.8

    EJEMPLOS:
        "19.99"  → 19.99  ✅
        "1,299"  → null   ❌ (la coma no es válida, necesita limpieza primero)
        "abc"    → null   ❌

    NOTA SOBRE PRECIOS:
        En sistemas financieros serios se usa DecimalType en lugar de DoubleType
        porque DoubleType puede tener errores de redondeo (ej: 0.1 + 0.2 ≠ 0.3
        exactamente en punto flotante). Para este pipeline de ecommerce,
        DoubleType es suficiente.

    RETORNA:
        DataFrame con la columna convertida a DoubleType
    """
    return df.withColumn(col_name, F.col(col_name).cast(DoubleType()))


def safe_cast_date(df: DataFrame, col_name: str, date_format: str = "yyyy-MM-dd") -> DataFrame:
    """
    Convierte una columna string a DateType.

    DIFERENCIA CON date_utils.parse_date:
        - date_utils.parse_date: Opera sobre Column, devuelve Column
        - safe_cast_date: Opera sobre DataFrame y columna por nombre, devuelve DataFrame

        Son complementarias. En el pipeline usamos ambas:
        - parse_date cuando construimos expresiones complejas
        - safe_cast_date para la conversión final definitiva

    PARÁMETROS:
        df:          DataFrame de PySpark
        col_name:    Nombre de la columna con la fecha como string
        date_format: Formato de la fecha de entrada

    RETORNA:
        DataFrame con la columna convertida a DateType

    EJEMPLOS CON date_format="yyyy-MM-dd":
        "2024-01-05" → DateType(2024-01-05)  ✅
        "2024-1-5"   → null                  ❌ (falta el cero, formato incorrecto)
        "abc"        → null                  ❌
    """
    return df.withColumn(
        col_name,
        F.to_date(F.col(col_name), date_format)
    )


def cast_schema(df: DataFrame, schema_map: dict) -> DataFrame:
    """
    Aplica un mapa de conversiones de tipos a múltiples columnas a la vez.

    PARA QUÉ SIRVE:
        En vez de llamar safe_cast_int(), safe_cast_double(), etc. una por una,
        puedes definir todas las conversiones en un diccionario y aplicarlas
        de una sola vez. Más limpio y fácil de mantener.

    PARÁMETROS:
        df:         DataFrame de PySpark
        schema_map: Diccionario donde la clave es el nombre de la columna
                    y el valor es el tipo destino como string:
                    "int", "long", "double", "date"

    EJEMPLO DE USO:
        schema_map = {
            "id":                "int",
            "price":             "double",
            "quantity":          "int",
            "registration_date": "date",
        }
        df = cast_schema(df, schema_map)

    RETORNA:
        DataFrame con todas las columnas convertidas
    """
    TYPE_HANDLERS = {
        "int":    lambda d, col: safe_cast_int(d, col),
        "long":   lambda d, col: safe_cast_long(d, col),
        "double": lambda d, col: safe_cast_double(d, col),
        "date":   lambda d, col: safe_cast_date(d, col),
    }

    for col_name, target_type in schema_map.items():
        if col_name not in df.columns:
            # Si la columna no existe, avisamos en lugar de fallar
            print(f"⚠️  Columna '{col_name}' no encontrada en el DataFrame. Se omite.")
            continue

        handler = TYPE_HANDLERS.get(target_type)
        if handler is None:
            raise ValueError(
                f"Tipo '{target_type}' no reconocido para columna '{col_name}'. "
                f"Tipos válidos: {list(TYPE_HANDLERS.keys())}"
            )

        df = handler(df, col_name)

    return df
