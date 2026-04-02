import os
from pathlib import Path

import requests
from google.cloud import bigquery
from dotenv import load_dotenv

# Répertoire Exoplanet_DBT
BASE_DIR = Path(__file__).resolve().parents[1]

# Charge explicitement le .env situé dans Exoplanet_DBT/.env
load_dotenv(BASE_DIR / ".env")

# Construit un chemin absolu vers la clé JSON
credentials_path = (BASE_DIR.parent / "credentials" / "cle_bigquery.json").resolve()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)

client = bigquery.Client()

table_id = "exoplanet-491915.raw_data.raw_exoplanets"

URL = (
    "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    "?query=select+*+from+pscomppars"
    "&format=json"
)

def ingest_data() -> None:
    response = requests.get(URL, timeout=120)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError("La réponse API doit être une liste JSON.")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_json(
        data,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    print(f"Succès : {len(data)} lignes chargées dans {table_id}.")

if __name__ == "__main__":
    ingest_data()