from transformacion.cleaning import clean_name, clean_country
from transformacion.cleaning.email_cleaner import clean_email


def run_transformacion(dataframes, spark):
    """
    Orquesta toda la transformación del pipeline.
    Paso a paso:
      1. Limpia nombre con clean_name
      2. Limpia email con clean_email
      3. Limpia country y city
      4. Retorna DataFrame limpio
    """
    df_customers = dataframes["customers"]

    # Limpieza de nombre (quita caracteres especiales, capitaliza)
    df_customers = clean_name(df_customers, "name")

    # Limpieza de email (valida formato, no quita @ ni .)
    df_customers = clean_email(df_customers)

    # Limpieza de country y city
    df_customers = clean_name(df_customers, "city")
    df_customers = clean_country(df_customers, spark)

    return {"customers": df_customers}