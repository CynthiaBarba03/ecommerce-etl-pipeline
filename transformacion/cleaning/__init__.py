# cleaning/ — Limpiadores por entidad
#
# Cada archivo en esta carpeta es responsable de la limpieza de UNA entidad.
# Todos usan las funciones de utils/ para las operaciones básicas.
#
# ESTRUCTURA:
#   customer_cleaner.py    ← limpieza de customers (la entidad principal)
#   country_normalizer.py  ← normalización ISO de países (reutilizable)
#
# CÓMO AGREGAR UNA NUEVA ENTIDAD:
#   1. Crear un archivo: {entidad}_cleaner.py
#   2. Importar funciones de utils/ según necesites
#   3. Crear run_{entidad}_cleaning(df, spark)
#   4. Exportarlo aquí en __all__

from .customer_cleaner import run_customer_cleaning
from .country_normalizer import normalize_country

__all__ = [
    "run_customer_cleaning",
    "normalize_country",
]