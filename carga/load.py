"""Carga Silver -> Postgres (warehouse).

Lee DataFrames ya transformados y los escribe en `retail_warehouse`.
Acepta DataFrame de Spark o de pandas. Configuracion por variables de entorno:

    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": os.getenv("PG_PORT", "5432"),
    "db": os.getenv("PG_DB", "retail_warehouse"),
    "user": os.getenv("PG_USER", "dataeng"),
    "password": os.getenv("PG_PASSWORD", "dataeng123"),
}


def _to_pandas(df):
    """Acepta Spark o pandas y devuelve pandas."""
    if hasattr(df, "toPandas"):  # DataFrame de Spark
        return df.toPandas()
    return df


def load_dataframe(df, table_name: str, if_exists: str = "append", chunksize: int = 1000) -> int:
    """Escribe `df` en `table_name`. Devuelve n. de filas cargadas."""
    pdf = _to_pandas(df)
    engine = create_engine(
        f"postgresql+psycopg://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['db']}"
    )
    with engine.begin() as conn:
        pdf.to_sql(table_name, conn, if_exists=if_exists, index=False, chunksize=chunksize)
    print(f"Cargadas {len(pdf)} filas en {table_name} (if_exists={if_exists}).")
    return len(pdf)


def main(clean_dataframes: dict | None = None, if_exists: str = "append"):
    """Punto de entrada para el DAG: `{"customers": df, ...}` -> Postgres."""
    if not clean_dataframes:
        print("Nada que cargar: no se recibieron DataFrames.")
        return {}
    return {
        entity: load_dataframe(df, entity, if_exists=if_exists)
        for entity, df in clean_dataframes.items()
    }


if __name__ == "__main__":
    main()
