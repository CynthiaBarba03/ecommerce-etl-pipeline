from pyspark.sql.functions import upper, trim, col, when

import pycountry
from pyspark.sql.functions import upper, trim, col, when


def _construir_mapeo_paises(spark):
    """Construye la tabla de referencia de países usando el catálogo oficial ISO (pycountry) + alias propios."""
    mapeo_dict = {}
    for pais in pycountry.countries:
        mapeo_dict[pais.name.upper()] = pais.name
        mapeo_dict[pais.alpha_2.upper()] = pais.name
        mapeo_dict[pais.alpha_3.upper()] = pais.name
        if hasattr(pais, "official_name"):
            mapeo_dict[pais.official_name.upper()] = pais.name

    # Alias propios: SOLO variantes confirmadas manualmente (nunca adivinadas automáticamente)
    alias_propios = {
        "UK": "United Kingdom",
        "U.S.A.": "United States",
        "U.S.A": "United States",
        "USA": "United States",
        "usa": "United States",
        # Aquí se van agregando los que confirmes con el paso de fuzzy matching del notebook exploratorio
    }
    mapeo_dict.update(alias_propios)

    return spark.createDataFrame(
        [(k, v) for k, v in mapeo_dict.items()],
        ["variante", "pais_estandar"]
    )


def limpiar_customers(df, spark):
    """
    Limpia y estandariza la tabla customers.
    - Normaliza country contra el catálogo oficial ISO (pycountry) + alias propios del dataset.
    - No inventa valores: Germany+ciudad-de-USA y nulos/vacíos -> UNKNOWN.
    - Cualquier valor no reconocido -> REVISAR_MANUAL (nunca se deja pasar silenciosamente).
    """
    mapeo_paises = _construir_mapeo_paises(spark)

    df = df.withColumn("country_clean", upper(trim(col("country"))))
    df = df.join(mapeo_paises, df.country_clean == mapeo_paises.variante, "left")

    df = df.withColumn(
        "country_final",
        when((col("country_clean").isNull()) | (col("country_clean") == ""), "UNKNOWN")
        .when(
            (col("country_clean") == "GERMANY") &
            (col("city").isin("Portland", "San Francisco", "Los Angeles", "Miami", "Denver", "Dallas", "Houston", "Phoenix", "los angeles")),
            "UNKNOWN"
        )
        .when(col("pais_estandar").isNotNull(), col("pais_estandar"))
        .otherwise("REVISAR_MANUAL")
    )

    df = df.drop("variante", "pais_estandar")
    return df
