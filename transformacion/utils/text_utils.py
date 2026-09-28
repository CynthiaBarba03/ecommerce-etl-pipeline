"""
text_utils.py — Funciones reutilizables de texto para PySpark
==============================================================

PROPÓSITO:
    Este archivo contiene operaciones GENÉRICAS de limpieza de texto.
    No sabe qué es un "customer" ni un "email". Solo sabe limpiar texto.
    Se puede importar en cualquier parte del pipeline.

POR QUÉ ESTÁ AQUÍ Y NO EN LOS CLEANERS:
    En el código anterior, estas 4 operaciones se repetían en name_cleaner.py,
    email_cleaner.py y country_cleaner.py:

        cleaned = F.trim(col)
        cleaned = F.regexp_replace(cleaned, r"\\s+", " ")

    Ahora están una sola vez aquí. Si necesitas cambiar cómo funciona el trim,
    cambias UN archivo y afecta a todo el pipeline. Eso es el principio DRY
    (Don't Repeat Yourself — No te repitas).

CÓMO SE USA:
    from transformacion.utils.text_utils import strip_and_normalize_spaces, normalize_case
    col_limpia = strip_and_normalize_spaces(F.col("name"))
"""

from pyspark.sql import functions as F
from pyspark.sql import Column


def strip_and_normalize_spaces(col: Column) -> Column:
    """
    Elimina espacios al inicio, al final y los espacios múltiples internos.

    EJEMPLOS:
        "  John   Smith  " → "John Smith"
        "  hello   world  " → "hello world"
        "NoEspacios" → "NoEspacios"  (sin cambios)

    POR QUÉ DOS PASOS:
        - F.trim() solo quita los espacios del borde (inicio y fin)
        - regexp_replace(r"\\s+", " ") quita los espacios INTERNOS dobles/triples
        - Necesitas los dos juntos para limpiar bien

    PARÁMETROS:
        col: Column de PySpark (se pasa como F.col("nombre_columna"))

    RETORNA:
        Column de PySpark con el texto limpio (sin espacios de más)
    """
    # Paso 1: quitar espacios al inicio y al final
    resultado = F.trim(col)
    # Paso 2: reemplazar dos o más espacios internos por uno solo
    #         \\s+ significa "uno o más espacios en blanco" (regex)
    resultado = F.regexp_replace(resultado, r"\s+", " ")
    return resultado


def normalize_case(col: Column, mode: str = "title") -> Column:
    """
    Normaliza la capitalización del texto.

    ESTO RESUELVE EL PROBLEMA DE: alicia = ALICIA = Alicia → Alicia

    MODOS DISPONIBLES:
        "title" (por defecto) → Primera letra de CADA PALABRA en mayúscula
                                "john smith" → "John Smith"
                                "o'brian" → "O'Brian" (respeta el apóstrofo)
                                "jean-claude" → "Jean-Claude" (respeta el guión)
        "upper"              → TODO EN MAYÚSCULAS
                                "john" → "JOHN"
        "lower"              → todo en minúsculas
                                "JOHN" → "john"

    POR QUÉ "title" PARA NOMBRES:
        La función F.initcap() de Spark es equivalente a "title case".
        Es la forma estándar de escribir nombres propios en inglés/español.

    PARÁMETROS:
        col:  Column de PySpark
        mode: str con el modo deseado ("title", "upper", "lower")

    RETORNA:
        Column de PySpark con la capitalización normalizada

    EJEMPLO DE USO:
        df = df.withColumn("name_clean", normalize_case(F.col("name")))
    """
    if mode == "title":
        # initcap = "initial capital" = primera letra de cada palabra en mayúscula
        return F.initcap(col)
    elif mode == "upper":
        return F.upper(col)
    elif mode == "lower":
        return F.lower(col)
    else:
        raise ValueError(f"Modo '{mode}' no reconocido. Usa: 'title', 'upper' o 'lower'")


def normalize_to_null(col: Column, replacement: str = "UNKNOWN") -> Column:
    """
    Convierte valores nulos O vacíos a un valor de reemplazo estándar.

    PROBLEMA QUE RESUELVE:
        En los datos crudos, "sin datos" puede venir de muchas formas:
        - None / null (nulo real de la base de datos)
        - "" (string vacío)
        - "  " (solo espacios)

        Esta función los unifica a "UNKNOWN" (o lo que tú elijas).

    POR QUÉ "UNKNOWN" Y NO null:
        - En un DataFrame de Spark, null puede causar problemas en JOINs
          y agregaciones.
        - "UNKNOWN" es un valor explícito que puedes filtrar y monitorear.
        - Es la convención estándar en pipelines de datos de producción.

    IMPORTANTE — EL ORDEN IMPORTA:
        Debes llamar strip_and_normalize_spaces() ANTES de normalize_to_null(),
        porque "   " (espacios) NO es null ni "", pero después del trim
        se convierte en "" y ENTONCES sí lo atrapamos.

        ✅ CORRECTO:
            col = strip_and_normalize_spaces(col)
            col = normalize_to_null(col)

        ❌ INCORRECTO:
            col = normalize_to_null(col)  ← no atrapa "   "
            col = strip_and_normalize_spaces(col)

    PARÁMETROS:
        col:         Column de PySpark
        replacement: El texto que pondrás en lugar de nulos/vacíos

    RETORNA:
        Column de PySpark donde null y "" son reemplazados por `replacement`
    """
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        replacement
    ).otherwise(col)


def remove_special_chars(col: Column, keep_chars: str = "") -> Column:
    """
    Elimina caracteres especiales, dejando solo letras, números y espacios.

    PROBLEMA QUE RESUELVE:
        Datos crudos a veces traen caracteres raros en los nombres:
        - "Joh@n#123" → "John"     (caracteres ilegales en nombres)
        - "O'Brian!"  → "O'Brian"  (! no válido, pero ' sí)
        - "Jean-Claude" → "Jean-Claude"  (guión válido en nombres compuestos)

    CÓMO FUNCIONA EL PATRÓN REGEX:
        [^a-zA-Z\\s] significa "cualquier carácter que NO sea letra ni espacio"
        El ^ dentro de [] significa NEGACIÓN.

        Si quieres mantener el apóstrofo y el guión:
            keep_chars = "'-"
            El patrón se vuelve: [^a-zA-Z\\s'-]

    PARÁMETROS:
        col:        Column de PySpark
        keep_chars: String con caracteres ADICIONALES que quieres conservar.
                    Por ejemplo: "'-" conserva apóstrofo y guión.
                    Por ejemplo: "." conserva el punto.
                    Por defecto vacío = solo letras y espacios.

    RETORNA:
        Column de PySpark sin los caracteres especiales

    EJEMPLOS:
        remove_special_chars(col)          → solo letras y espacios
        remove_special_chars(col, "'-")    → letras, espacios, ' y -
        remove_special_chars(col, ".")     → letras, espacios y .
    """
    # Construimos el patrón regex dinámicamente según keep_chars
    # Escapamos los caracteres especiales de regex para que no rompan el patrón
    pattern = f"[^a-zA-Z\\s{keep_chars}]"
    return F.regexp_replace(col, pattern, "")
