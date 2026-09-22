import pycountry
from pyspark.sql.functions import upper, trim, col, when
from transformacion.cleaning.name_cleaner import clean_name
from transformacion.models.customer import CLEANING_RULES


def _construir_mapeo_paises(spark):
    """Tabla de referencia oficial ISO (pycountry) + alias propios del dataset."""
    mapeo_dict = {}
    for pais in pycountry.countries:
        mapeo_dict[pais.name.upper()] = pais.name
        mapeo_dict[pais.alpha_2.upper()] = pais.name
        mapeo_dict[pais.alpha_3.upper()] = pais.name
        if hasattr(pais, "official_name"):
            mapeo_dict[pais.official_name.upper()] = pais.name

    alias_propios = {
        "UK": "United Kingdom",
        "U.S.A.": "United States",
        "U.S.A": "United States",
        "USA": "United States",
        "usa": "United States",
    }
    mapeo_dict.update(alias_propios)

    return spark.createDataFrame(
        [(k, v) for k, v in mapeo_dict.items()],
        ["variante", "pais_estandar"]
    )


def clean_country(df, spark):
    """
    Limpia y estandariza la columna country.
    1. Normaliza contra el catálogo oficial ISO (pycountry)
    2. Nulos/vacíos -> UNKNOWN
    3. Valores no reconocidos -> REVISAR_MANUAL (nunca se deja pasar silenciosamente)
    """
    # Limpieza de texto general (espacios de más, capitalización)
    df = clean_name(df, "country", replacement="UNKNOWN")

    # Normalizar contra catálogo ISO
    mapeo_paises = _construir_mapeo_paises(spark)
    df = df.withColumn("country_clean", upper(trim(col("country_clean"))))
    df = df.join(mapeo_paises, df.country_clean == mapeo_paises.variante, "left")

    df = df.withColumn(
        "country_final",
        when((col("country_clean").isNull()) | (col("country_clean") == ""), "UNKNOWN")
        .when(
            (col("country_clean") == "GERMANY") &
            (col("city_clean").isin(
                "Portland", "San Francisco", "Los Angeles", "Miami",
                "Denver", "Dallas", "Houston", "Phoenix", "Los Angeles"
            )),
            "UNKNOWN"
        )
        .when(col("pais_estandar").isNotNull(), col("pais_estandar"))
        .otherwise("REVISAR_MANUAL")
    )

    return df.drop("variante", "pais_estandar")