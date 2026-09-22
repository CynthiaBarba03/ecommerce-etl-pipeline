from transformacion.cleaning import clean_name, clean_country


def run_transformacion(dataframes, spark):
    """
    Orquesta toda la transformación del pipeline.
    Paso a paso:
      1. Limpia nombres (first_name, last_name) con clean_name
      2. Limpia email con clean_name
      3. Limpia country y city
      4. Retorna DataFrame limpio
    """
    df_customers = dataframes["customers"]

    # Limpieza de textos (nombres, email, city)
    df_customers = clean_name(df_customers, "first_name")
    df_customers = clean_name(df_customers, "last_name")
    df_customers = clean_name(df_customers, "email")
    df_customers = clean_name(df_customers, "city")

    # Limpieza de país (usa pycountry)
    df_customers = clean_country(df_customers, spark)

    return {"customers": df_customers}