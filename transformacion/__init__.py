# transformacion/ — Paquete principal de transformación
#
# Punto de entrada público del módulo de transformación.
# Solo exponemos lo que el código exterior (extraccion, carga, dags)
# necesita usar. El resto es "interno" al paquete.

from .models.customer import CUSTOMER_SCHEMA, CLEANING_RULES
from .cleaning.customer_cleaner import run_customer_cleaning
from .pipeline_transformacion import run_transformacion

__all__ = [
    "CUSTOMER_SCHEMA",
    "CLEANING_RULES",
    "run_customer_cleaning",
    "run_transformacion",
]
