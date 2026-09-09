from transformacion.limpieza_customers import limpiar_customers
# from transformacion.limpieza_products import limpiar_products

def run_transformacion(dataframes):
    resultados = {}
    resultados["customers"] = limpiar_customers(dataframes["customers"])
    # resultados["products"] = limpiar_products(dataframes["products"])
    return resultados