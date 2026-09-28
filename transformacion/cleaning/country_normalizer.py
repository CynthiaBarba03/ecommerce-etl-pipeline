"""
country_normalizer.py — Estandarización de países contra ISO 3166
=================================================================

PROPÓSITO:
    Normalizar los nombres de países contra el catálogo oficial ISO 3166.
    Este archivo se separa del customer_cleaner porque la lógica de países
    es compleja e independiente: podría reutilizarse para suppliers,
    shipping addresses, etc.

POR QUÉ ESTE ARCHIVO SIGUE EXISTIENDO (no se movió a utils):
    La normalización de países NO es una operación genérica de texto.
    Es lógica de negocio que:
    1. Usa pycountry (librería externa de datos de países)
    2. Crea una tabla temporal de Spark (join)
    3. Tiene reglas propias de negocio (detección de anomalías como
       Germany + ciudad americana)

    Eso la hace específica del dominio "geografía/países", no una
    utilidad de texto genérica.
"""

import pycountry
from pyspark.sql import functions as F, DataFrame, SparkSession


# Alias manuales para variantes que pycountry no reconoce por defecto.
# Estas son variaciones comunes en datos de ecommerce del mundo real.
# Se pueden agregar más según los datos que encuentres.
COUNTRY_ALIASES: dict = {
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "U.S.A.": "United States",
    "U.S.A": "United States",
    "USA": "United States",
    "EEUU": "United States",
    "US": "United States",
}


def _build_country_lookup_table(spark: SparkSession) -> DataFrame:
    """
    Construye una tabla de referencia de países aceptados.

    CÓMO FUNCIONA:
        pycountry contiene todos los países del mundo según el estándar ISO 3166.
        Para cada país, cargamos todas sus variantes conocidas:
            - Nombre completo: "Germany", "United States of America"
            - Nombre oficial: "Federal Republic of Germany" (si existe)
            - Código ISO-2: "DE", "US"
            - Código ISO-3: "DEU", "USA"

        Creamos un diccionario { variante_en_mayusculas → nombre_oficial }
        Luego lo convertimos a DataFrame de Spark para hacer un JOIN eficiente.

    POR QUÉ UN JOIN Y NO UN UDF:
        Un UDF (User Defined Function) en Python se ejecuta fila por fila
        y es muy lento en datasets grandes. Un JOIN de Spark se distribuye
        entre todos los nodos del cluster → mucho más rápido.

    RETORNA:
        DataFrame de Spark con columnas:
            - variante:      El texto tal como puede venir en los datos
            - pais_estandar: El nombre oficial del país
    """
    lookup: dict = {}

    # Cargar todos los países de pycountry
    for country in pycountry.countries:
        nombre_upper = country.name.upper()
        lookup[nombre_upper] = country.name
        lookup[country.alpha_2.upper()] = country.name   # "US" → "United States"
        lookup[country.alpha_3.upper()] = country.name   # "USA" → "United States"

        if hasattr(country, "official_name"):
            lookup[country.official_name.upper()] = country.name

    # Agregar aliases manuales (sobrescriben si hay conflicto)
    lookup.update(COUNTRY_ALIASES)

    # Convertir el diccionario a DataFrame de Spark
    # [(clave, valor), (clave, valor), ...] → DataFrame
    return spark.createDataFrame(
        [(variante, estandar) for variante, estandar in lookup.items()],
        schema=["variante", "pais_estandar"]
    )


def normalize_country(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Estandariza la columna 'country' contra el catálogo ISO 3166.

    ESTRATEGIA DE LIMPIEZA:
        1. Normalizar texto: quitar espacios, pasar a mayúsculas
           (para que "germany" == "GERMANY" == "Germany" → "GERMANY" para el lookup)
        2. JOIN contra la tabla de referencia
        3. Asignar resultado:
           - Si está en la tabla → nombre oficial ISO
           - Si es nulo/vacío → "UNKNOWN"
           - Si no se reconoció → "REVISAR_MANUAL"

    SOBRE "REVISAR_MANUAL":
        Es mejor marcar explícitamente los valores no reconocidos que
        ignorarlos silenciosamente. En un pipeline de producción real,
        estos valores se enviarían a una tabla de anomalías para revisión
        manual o enriquecimiento de datos.

    PARÁMETROS:
        df:    DataFrame de PySpark con columna "country"
        spark: SparkSession (necesaria para crear la tabla de referencia)

    RETORNA:
        DataFrame con columnas:
            - "country_clean":  El texto normalizado y estandarizado
            - "country_final":  El nombre oficial ISO (o UNKNOWN/REVISAR_MANUAL)
    """
    # Paso 1: Normalizar el texto para el lookup (TODO EN MAYÚSCULAS para comparar)
    df = df.withColumn(
        "country_lookup_key",
        F.upper(F.trim(F.col("country")))
    )

    # Paso 2: Construir y hacer JOIN con la tabla de referencia ISO
    lookup_table = _build_country_lookup_table(spark)
    df = df.join(
        lookup_table,
        df["country_lookup_key"] == lookup_table["variante"],
        how="left"  # LEFT JOIN: conservamos TODOS los customers aunque el país no se reconozca
    )

    # Paso 3: Determinar el valor final según el resultado del JOIN
    df = df.withColumn(
        "country_clean",
        F.when(
            F.col("country_lookup_key").isNull() | (F.col("country_lookup_key") == ""),
            "UNKNOWN"
        )
        .when(
            F.col("pais_estandar").isNotNull(),
            F.col("pais_estandar")   # ← nombre oficial del país
        )
        .otherwise("REVISAR_MANUAL")  # ← no se reconoció, marcar para revisión
    )

    # Limpiar columnas temporales del proceso
    return df.drop("variante", "pais_estandar", "country_lookup_key")
