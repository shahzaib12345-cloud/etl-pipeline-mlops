"""
Daily currency-exchange-rate ETL: Frankfurter API -> transform -> MongoDB.

Three tasks, connected with named XComs:
    extract_exchange_rates  --(key: raw_rates)-->         transform_exchange_rates
    transform_exchange_rates --(key: transformed_rates)--> load_to_mongodb
"""
import os
from datetime import datetime, timezone

import pendulum
import requests
from pymongo import ASCENDING, MongoClient, ReplaceOne

# Works on Airflow 3 (Astro Runtime 3.x) and Airflow 2 (Astro Runtime 12/13).
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

# ---------- Config ----------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
SOURCE_NAME = "Frankfurter API"

MONGO_URI = os.environ.get(
    "MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin"
)
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys
XCOM_RAW = "raw_rates"
XCOM_TRANSFORMED = "transformed_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily Frankfurter exchange rates -> MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(**context):
        """Call the Frankfurter v2 API and push the raw response under a named XCom key."""
        response = requests.get(
            API_URL,
            params={"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)},
            timeout=30,
        )
        response.raise_for_status()
        raw_rates = response.json()  # v2 returns a list: [{date, base, quote, rate}, ...]

        if not isinstance(raw_rates, list) or not raw_rates:
            raise ValueError(f"Unexpected or empty API response: {raw_rates!r}")

        context["ti"].xcom_push(key=XCOM_RAW, value=raw_rates)

    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(**context):
        """Reshape each API row into the required document format."""
        ti = context["ti"]
        raw_rates = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW)
        if not raw_rates:
            raise ValueError("No raw rates found in XCom")

        retrieved_at = (
            datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat()
        )

        transformed = [
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": float(row["rate"]),
                "rate_date": row["date"],
                "source": SOURCE_NAME,
                "retrieved_at": retrieved_at,
            }
            for row in raw_rates
        ]

        ti.xcom_push(key=XCOM_TRANSFORMED, value=transformed)

    @task(task_id="load_to_mongodb")
    def load_to_mongodb(**context):
        """Upsert the transformed documents into MongoDB (safe to re-run)."""
        documents = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED
        )
        if not documents:
            raise ValueError("No transformed rates found in XCom")

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]
            collection.create_index(
                [
                    ("base_currency", ASCENDING),
                    ("target_currency", ASCENDING),
                    ("rate_date", ASCENDING),
                ],
                unique=True,
            )
            operations = [
                ReplaceOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                    },
                    d,
                    upsert=True,
                )
                for d in documents
            ]
            result = collection.bulk_write(operations)
            print(
                f"Loaded {len(documents)} docs into {MONGO_DB}.{MONGO_COLLECTION} "
                f"(upserted={result.upserted_count}, modified={result.modified_count})"
            )
        finally:
            client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()
