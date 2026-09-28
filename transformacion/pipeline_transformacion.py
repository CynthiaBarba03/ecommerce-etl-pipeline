"""
pipeline_transformacion.py — Orquestador del pipeline de transformación
=======================================================================

PROPÓSITO:
    Este archivo es el "director de orquesta". No hace nada por sí mismo,
    solo coordina quién hace qué y en qué orden.

    PRINCIPIO: Este archivo debe ser tan simple que cualquier persona
    (incluso sin saber Python) pueda entender el flujo con solo leerlo.

¿POR QUÉ UN ORQUESTADOR SEPARADO?
    Si el día de mañana agregas products, orders, inventory, etc.,
    solo necesitas añadir una línea aquí:
        df_products = run_product_cleaning(dataframes["products"], spark)

    Sin tocar nada más. Eso es escalabilidad.

FLUJO:
    dataframes (dict) → limpieza de cada entidad → dataframes limpios (dict)
    
    Entrada:  { "customers": df_crudo, ... }
    Salida:   { "customers": df_limpio, ... }
"""

from transformacion.cleaning.customer_cleaner import run_customer_cleaning


def run_transformacion(dataframes: dict, spark) -> dict:
    """
    Orquesta la transformación completa de todas las entidades del pipeline.

    PARÁMETROS:
        dataframes: Diccionario con DataFrames crudos por entidad.
                    Ejemplo: {"customers": df_customers, "products": df_products}
        spark:      SparkSession activa (requerida para operaciones con joins)

    RETORNA:
        Diccionario con DataFrames limpios.
        Mismas claves que la entrada, pero con datos transformados.

    CÓMO AÑADIR UNA NUEVA ENTIDAD:
        1. Crear transformacion/cleaning/product_cleaner.py
        2. Importar aquí: from transformacion.cleaning.product_cleaner import run_product_cleaning
        3. Añadir: dataframes_clean["products"] = run_product_cleaning(...)
    """
    # Usamos un diccionario separado para los resultados limpios
    # (no modificamos el original, buena práctica para debugging)
    dataframes_clean = {}

    # === CUSTOMERS ===
    if "customers" in dataframes:
        print("▶️  Transformando: customers...")
        dataframes_clean["customers"] = run_customer_cleaning(
            dataframes["customers"],
            spark
        )
        print("✅ customers completado.")

    # === AQUÍ IRÁN LAS DEMÁS ENTIDADES (cuando las implementes) ===
    # if "products" in dataframes:
    #     dataframes_clean["products"] = run_product_cleaning(dataframes["products"], spark)
    #
    # if "orders" in dataframes:
    #     dataframes_clean["orders"] = run_order_cleaning(dataframes["orders"], spark)

    return dataframes_clean