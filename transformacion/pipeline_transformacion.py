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
from transformacion.cleaning.coupon_cleaner import run_coupon_cleaning
from transformacion.cleaning.category_cleaner import run_category_cleaning
from transformacion.cleaning.inventory_cleaner import run_inventory_cleaning


def run_transformacion(dataframes: dict, spark) -> dict:
    """
    Orquesta la transformación completa de todas las entidades del pipeline.

    PARÁMETROS:
        dataframes: Diccionario con DataFrames crudos por entidad.
                    Ejemplo: {"customers": df_customers, "coupons": df_coupons, ...}
        spark:      SparkSession activa (requerida para operaciones con joins)

    RETORNA:
        Diccionario con DataFrames limpios.
        Mismas claves que la entrada, pero con datos transformados.
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

    # === COUPONS ===
    if "coupons" in dataframes:
        print("▶️  Transformando: coupons...")
        dataframes_clean["coupons"] = run_coupon_cleaning(
            dataframes["coupons"],
            spark
        )
        print("✅ coupons completado.")

    # === CATEGORIES ===
    if "categories" in dataframes:
        print("▶️  Transformando: categories...")
        dataframes_clean["categories"] = run_category_cleaning(
            dataframes["categories"],
            spark
        )
        print("✅ categories completado.")

    # === INVENTORY ===
    if "inventory" in dataframes:
        print("▶️  Transformando: inventory...")
        dataframes_clean["inventory"] = run_inventory_cleaning(
            dataframes["inventory"],
            spark
        )
        print("✅ inventory completado.")

    return dataframes_clean