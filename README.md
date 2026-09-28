# ecommerce-pipeline

Pipeline ETL: **API sucia → Bronze → Silver → Postgres**.

```
API REST ──extract──▶ Bronze (Azure Blob, JSON crudo + watermarks)
                         ──transform (PySpark)──▶ Silver (parquet limpio)
                                                      ──load──▶ Postgres
```

La orquestación en Azure (Data Factory / Databricks Jobs) aún no está definida:
cada fase se ejecuta por separado llamando a su `main()`. Nada de Airflow en este repo.

## Mapa del proyecto

| Ruta | Qué es |
|---|---|
| `extraccion/extraction.py` | API → Bronze, con `main()` importable y watermark incremental |
| `transformacion/cleaning/` | Un cleaner por entidad (`run_*_cleaning`) + `common.py` (funciones compartidas) |
| `transformacion/models/` | Esquemas y reglas por entidad (qué limpiar y a qué tipo convertir) |
| `transformacion/utils/` | Operaciones genéricas: texto, fechas, validación, casts |
| `transformacion/pipeline_transformacion.py` | Orquestador: `dict` de DFs crudos → `dict` de DFs limpios |
| `carga/load.py` | Silver → Postgres (`load_dataframe`, config por env) |
| `tests/` | `pytest tests/ -v` |
| `notebooks_exploratorios/` | Solo exploración; no forman parte del pipeline |

## Regla de dependencias

`utils/` no importa a nadie · `models/` no importa a nadie ·
`cleaning/common.py` solo usa `utils/` · cada cleaner usa `common` + `utils` + su `models/` ·
`pipeline_transformacion` solo llama a los `run_*_cleaning`.

## Correr en local

```bash
cp .env.example .env   # completar secretos (ver Seguridad)
docker compose up --build   # Postgres + pgAdmin (http://localhost:5050)
pytest tests/ -v
```

## Seguridad

La `AccountKey` de Azure **estuvo hardcodeada en `docker-compose.yml`** (ese archivo sí va a
git). Ya se eliminó de ahí: los secretos viven solo en `.env` (ignorado por git).
Como la clave quedó expuesta en el historial, **rotala en Azure**: Storage account → Access keys → Regenerate.
