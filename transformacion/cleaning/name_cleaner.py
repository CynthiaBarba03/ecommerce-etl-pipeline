from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_name(df, column_name, replacement="UNKNOWN"):
    """
    Limpia una columna de nombres en un DataFrame.

    Reglas aplicadas:
    1. Elimina caracteres especiales excepto apóstrofo (') y guión (-)
       -> O'Brian, Jean-Claude se preservan
    2. Elimina espacios en blanco de más
       ->  "  John   Smith  " -> "John Smith"
    3. Si queda vacío o nulo -> replacement (por defecto "UNKNOWN")
    4. Capitaliza correctamente cada palabra
       -> "john" -> "John", "o'brian" -> "O'Brian", "jean-claude" -> "Jean-Claude"
    """
    col = F.col(column_name)

    # Paso 1: Eliminar caracteres especiales
    # [a-zA-Z\s'-] = letras, espacios, apóstrofo, guión
    # Todo lo que NO esté en esa lista se elimina
    # Ej: "Joh@n#123" -> "John"  |  "O'Brian!" -> "O'Brian"
    cleaned = F.regexp_replace(col, r"[^a-zA-Z\s'-]", "")

    # Paso 2: Quitar espacios de más
    # trim() = espacios al inicio y al final
    # regexp_replace multiple spaces -> single space
    cleaned = F.trim(cleaned)
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")

    # Paso 3: Si queda vacío o nulo -> UNKNOWN
    cleaned = F.when(
        (F.trim(cleaned) == "") | (F.trim(cleaned).isNull()),
        replacement
    ).otherwise(cleaned)

    # Paso 4: Capitalizar correctamente
    # initcap pone la primera letra de cada palabra en mayúscula
    # y el resto en minúscula
    # "o'brian" -> "O'Brian" (correcto)
    # "jean-claude" -> "Jean-Claude" (correcto)
    # "john smith" -> "John Smith" (correcto)
    cleaned = F.initcap(cleaned)

    # Renombrar la columna original con sufijo _clean
    df = df.withColumn(f"{column_name}_clean", cleaned)

    return df


def clean_names_dataframe(df, config=None):
    """
    Limpia todas las columnas de nombre configuradas.
    Lee el diccionario CLEANING_RULES del modelo customer.py.
    """
    from transformacion.models.customer import CLEANING_RULES

    if config is None:
        config = CLEANING_RULES

    for column_name, rules in config.items():
        if rules.get("remove_special_chars") or rules.get("strip_whitespace"):
            df = clean_name(
                df,
                column_name,
                replacement=rules.get("null_replacement", "UNKNOWN")
            )

    return df
