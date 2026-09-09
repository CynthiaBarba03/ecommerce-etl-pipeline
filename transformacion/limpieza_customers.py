from pyspark.sql.functions import upper, trim, col, when


def limpiar_customers(df):
    """
    Limpia y estandariza la tabla customers.
    - Normaliza country a: USA, Canada, Mexico, UK, UNKNOWN
    - Cualquier valor no reconocido se marca como REVISAR_MANUAL (no se adivina)
    """
    df = df.withColumn("country_clean", upper(trim(col("country"))))

    df = df.withColumn(
        "country_final",
        when(col("country_clean").isin("USA", "US", "U.S.A", "U.S.A.", "UNITED STATES"), "USA")
        .when(col("country_clean").isin("CANADA"), "Canada")
        .when(col("country_clean").isin("MEXICO"), "Mexico")
        .when(col("country_clean").isin("UK", "UNITED KINGDOM"), "UK")
        .when(
            (col("country_clean") == "GERMANY") &
            (col("city").isin("Portland", "San Francisco", "Los Angeles", "Miami", "Denver", "Dallas", "Houston", "Phoenix", "los angeles")),
            "UNKNOWN"
        )
        .when((col("country_clean").isNull()) | (col("country_clean") == ""), "UNKNOWN")
        .otherwise("REVISAR_MANUAL")  # <- antes decía .otherwise(col("country_clean")), ahora marca explícito
    )

    return df