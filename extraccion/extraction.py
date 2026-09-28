"""Extraccion API -> Bronze (Azure Blob Storage).

Cada entidad se pagina (`skip`/`limit`), opcionalmente con `since` para
incremental, y se sube cruda a `bronze/{entidad}/ingestion_date=YYYY-MM-DD/`.
El avance se guarda en `control/watermarks/{entidad}.json`.
"""

import json
import os
from datetime import datetime, timezone

import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("API_BASE_URL")
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# NOTA: la API usa /shipping (singular), no /shipments -> 404 con plural.
ENTITIES = [
    "customers",
    "products",
    "reviews",
    "categories",
    "suppliers",
    "payments",
    "inventory",
    "shipping",
    "coupons",
]


def _blob_service() -> BlobServiceClient:
    if not CONN_STR:
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en el entorno (.env).")
    if not API_BASE:
        raise RuntimeError("Falta API_BASE_URL en el entorno (.env).")
    return BlobServiceClient.from_connection_string(CONN_STR)


def get_watermark(blob_service: BlobServiceClient, entity_name: str):
    """Ultimo `last_successful_run` guardado, o None si es la primera corrida."""
    blob_client = blob_service.get_container_client("control").get_blob_client(
        f"watermarks/{entity_name}.json"
    )
    if not blob_client.exists():
        return None
    return json.loads(blob_client.download_blob().readall())["last_successful_run"]


def save_watermark(blob_service: BlobServiceClient, entity_name: str, run_start_time: str):
    """Guarda el watermark. Llamar SOLO si la extraccion fue exitosa."""
    blob_client = blob_service.get_container_client("control").get_blob_client(
        f"watermarks/{entity_name}.json"
    )
    blob_client.upload_blob(json.dumps({"last_successful_run": run_start_time}), overwrite=True)


def extract_entity(entity_name: str, since: str | None = None, limit: int = 100) -> list:
    """Pagina toda la entidad; filtra por `since` solo si hay watermark previo."""
    all_records = []
    skip = 0
    while True:
        params = {"skip": skip, "limit": limit}
        if since is not None:
            params["since"] = since
        print(f"Pidiendo {entity_name}: skip={skip}, since={since}...")
        response = requests.get(f"{API_BASE}/{entity_name}", params=params, timeout=30)
        response.raise_for_status()
        body = response.json()
        page_data = body["data"]  # la lista real viene anidada en "data"
        print(f"  -> llegaron {len(page_data)} registros (total en API: {body['total']})")
        if not page_data:
            break
        all_records.extend(page_data)
        skip += limit
    return all_records


def upload_to_bronze(blob_service: BlobServiceClient, entity_name: str, data: list, ingestion_date: str):
    """Sube los datos crudos a bronze particionado por fecha."""
    blob_path = f"{entity_name}/ingestion_date={ingestion_date}/{entity_name}.json"
    blob_service.get_container_client("bronze").get_blob_client(blob_path).upload_blob(
        json.dumps(data, indent=2), overwrite=True
    )
    print(f"Subido a bronze en: {blob_path}")


def run_entity(blob_service: BlobServiceClient, entity_name: str):
    now = datetime.now(timezone.utc)
    run_start_time = now.strftime("%Y-%m-%d %H:%M:%S")
    ingestion_date = now.date().isoformat()
    since = get_watermark(blob_service, entity_name)
    data = extract_entity(entity_name, since=since)
    if data:
        upload_to_bronze(blob_service, entity_name, data, ingestion_date)
    else:
        print(f"Sin registros nuevos para {entity_name}.")
    # La corrida fue exitosa (con o sin datos) -> avanzar watermark.
    save_watermark(blob_service, entity_name, run_start_time)
    print(f"--- {entity_name} completado ---\n")


def main(entities: list | None = None):
    blob_service = _blob_service()
    for entity_name in entities or ENTITIES:
        run_entity(blob_service, entity_name)


if __name__ == "__main__":
    main()
