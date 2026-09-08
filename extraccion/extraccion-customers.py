import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# 1. Cargar credenciales del .env
load_dotenv()
conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
api_base = os.getenv("API_BASE_URL")

blob_service = BlobServiceClient.from_connection_string(conn_str)

# Lista de entidades a extraer (confirma que todos los nombres coincidan con /docs)
# NOTA: la API usa /shipping (singular), no /shipments -> 404 si usas plural
entities = ["customers", "products", "reviews", "categories", "suppliers", "payments", "inventory", "shipping", "coupons"]


def get_watermark(entity_name):
    """Lee el último watermark guardado para esta entidad. Si no existe, devuelve None (primera corrida)."""
    container_client = blob_service.get_container_client("control")
    blob_client = container_client.get_blob_client(f"watermarks/{entity_name}.json")

    if not blob_client.exists():
        return None  # nunca se ha corrido esta entidad -> full load

    content = blob_client.download_blob().readall()
    watermark_data = json.loads(content)
    return watermark_data["last_successful_run"]


def save_watermark(entity_name, run_start_time):
    """Guarda el watermark para la próxima corrida (se llama SOLO si la extracción fue exitosa)."""
    container_client = blob_service.get_container_client("control")
    blob_client = container_client.get_blob_client(f"watermarks/{entity_name}.json")

    watermark_data = {"last_successful_run": run_start_time}
    blob_client.upload_blob(json.dumps(watermark_data), overwrite=True)


def extract_entity(entity_name, since=None, limit=100):
    """Extrae todos los registros de una entidad, paginando, y opcionalmente filtrando por 'since'."""
    all_records = []
    skip = 0

    while True:
        url = f"{api_base}/{entity_name}"
        params = {"skip": skip, "limit": limit}
        if since is not None:
            params["since"] = since  # solo se agrega el filtro si hay watermark previo

        print(f"Pidiendo {entity_name}: skip={skip}, since={since}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        response_json = response.json()
        page_data = response_json["data"]  # la lista real está anidada en "data"

        print(f"  -> llegaron {len(page_data)} registros (total en API: {response_json['total']})")

        if len(page_data) == 0:
            break

        all_records.extend(page_data)
        skip += limit

    return all_records


def upload_to_bronze(entity_name, data, ingestion_date):
    """Sube los datos crudos extraídos al container bronze, particionado por fecha."""
    container_client = blob_service.get_container_client("bronze")
    blob_path = f"{entity_name}/ingestion_date={ingestion_date}/{entity_name}.json"
    blob_client = container_client.get_blob_client(blob_path)

    blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
    print(f"Subido a bronze en: {blob_path}")


# --- Proceso principal ---
for entity_name in entities:
    # Paso 1: capturar la hora de inicio ANTES de pedir nada (será el próximo watermark)
    run_start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ingestion_date = datetime.utcnow().date().isoformat()

    # Paso 2-4: leer watermark previo (o None si es la primera vez)
    since = get_watermark(entity_name)

    # Paso 5: extraer y subir
    data = extract_entity(entity_name, since=since)

    if len(data) > 0:
        upload_to_bronze(entity_name, data, ingestion_date)
        # Paso 6: solo actualizamos el watermark si hubo datos (o incluso si vino vacío, ya que igual "corrió bien")
        save_watermark(entity_name, run_start_time)
    else:
        print(f"No hubo registros nuevos para {entity_name}, se actualiza el watermark igual (corrida exitosa sin cambios).")
        save_watermark(entity_name, run_start_time)

    print(f"--- {entity_name} completado ---\n")