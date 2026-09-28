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

from transformacion.cleaning.category_cleaner import run_category_cleaning
from transformacion.cleaning.coupon_cleaner import run_coupon_cleaning
from transformacion.cleaning.customer_cleaner import run_customer_cleaning
from transformacion.cleaning.inventory_cleaner import run_inventory_cleaning
from transformacion.cleaning.payment_cleaner import run_payment_cleaning
from transformacion.cleaning.product_cleaner import run_product_cleaning
from transformacion.cleaning.review_cleaner import run_review_cleaning
from transformacion.cleaning.shipping_cleaner import run_shipping_cleaning
from transformacion.cleaning.supplier_cleaner import run_supplier_cleaning


CLEANERS = {
    "customers": run_customer_cleaning,
    "coupons": run_coupon_cleaning,
    "categories": run_category_cleaning,
    "inventory": run_inventory_cleaning,
    "payments": run_payment_cleaning,
    "products": run_product_cleaning,
    "reviews": run_review_cleaning,
    "shipping": run_shipping_cleaning,
    "suppliers": run_supplier_cleaning,
}


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
    dataframes_clean = {}
    for entity_name, cleaner in CLEANERS.items():
        if entity_name not in dataframes:
            continue
        print(f"Transformando: {entity_name}...")
        dataframes_clean[entity_name] = cleaner(dataframes[entity_name], spark)
        print(f"{entity_name} completado.")

    return dataframes_clean