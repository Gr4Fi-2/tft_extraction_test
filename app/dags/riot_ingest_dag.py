from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging
from typing import Dict, Any, List
from pathlib import Path
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
RIOT_REGION = os.getenv("RIOT_REGION", "euw1")
RIOT_PUUID = os.getenv("RIOT_PUUID")
HEADERS = {"X-Riot-Token": RIOT_API_KEY}
BASE_URL = f"https://{RIOT_REGION}.api.riotgames.com/tft"
DATA_DIR = Path(os.getenv("DATA_STORAGE_PATH", "/opt/airflow/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "tft_matches.db"


class RiotAPIError(Exception):
    """Custom exception for Riot API errors."""

    pass


def init_db() -> None:
    """Initialize DuckDB database with required tables."""
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id VARCHAR,
            game_datetime TIMESTAMP,
            placement INTEGER,
            level INTEGER,
            gold_left INTEGER,
            last_round INTEGER,
            players_eliminated INTEGER,
            time_eliminated INTEGER,
            raw_data JSON
        )
    """
    )
    conn.close()


def fetch_match_ids(puuid: str, start_time: int, end_time: int) -> List[str]:
    """
    Fetch match IDs for a given PUUID and time range.

    Args:
        puuid: Player's PUUID
        start_time: Start timestamp in milliseconds
        end_time: End timestamp in milliseconds

    Returns:
        List of match IDs
    """
    url = f"{BASE_URL}/match/v1/matches/by-puuid/{puuid}/ids"
    params = {"startTime": start_time, "endTime": end_time, "count": 100}

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def fetch_match_details(match_id: str) -> Dict[str, Any]:
    """
    Fetch detailed match data for a specific match ID.

    Args:
        match_id: The match ID to fetch

    Returns:
        Match data dictionary
    """
    url = f"{BASE_URL}/match/v1/matches/{match_id}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def process_match_data(match_data: Dict[str, Any], puuid: str) -> Dict[str, Any]:
    """
    Extract relevant data from match response for the given player.

    Args:
        match_data: Raw match data from API
        puuid: Player's unique PUUID

    Returns:
        Processed match data
    """
    metadata = match_data.get("metadata", {})
    info = match_data.get("info", {})

    participants = info.get("participants", [])
    player = next((p for p in participants if p.get("puuid") == puuid), {})

    return {
        "match_id": metadata.get("match_id"),
        "game_datetime": datetime.fromtimestamp(info.get("game_datetime", 0) / 1000),
        "placement": player.get("placement"),
        "level": player.get("level"),
        "gold_left": player.get("gold_left"),
        "last_round": player.get("last_round"),
        "players_eliminated": player.get("players_eliminated"),
        "time_eliminated": player.get("time_eliminated"),
        "raw_data": json.dumps(match_data),
    }


def fetch_and_store_matches(**context) -> None:
    """
    Fetch TFT match data and store in DuckDB.

    This function can be used for both historical and daily updates.
    For historical data, pass execution_date in context.
    For daily updates, it will fetch the previous day's matches.
    """
    try:
        # Initialize database
        init_db()

        # Get time range
        execution_date = context.get("execution_date", datetime.now())
        if context.get("is_historical", False):
            start_time = int((execution_date - timedelta(days=30)).timestamp() * 1000)
            end_time = int(execution_date.timestamp() * 1000)
        else:
            start_time = int((execution_date - timedelta(days=1)).timestamp() * 1000)
            end_time = int(execution_date.timestamp() * 1000)

        if not RIOT_PUUID:
            raise RiotAPIError("RIOT_PUUID environment variable not set")

        puuid = RIOT_PUUID

        # Fetch match IDs
        match_ids = fetch_match_ids(puuid, start_time, end_time)
        logger.info(f"Found {len(match_ids)} matches")

        # Fetch and store match details
        conn = duckdb.connect(str(DB_PATH))
        for match_id in match_ids:
            try:
                match_data = fetch_match_details(match_id)
                processed_data = process_match_data(match_data, puuid)

                # Insert into database
                conn.execute(
                    """
                    INSERT INTO matches (
                        match_id, game_datetime, placement, level, gold_left,
                        last_round, players_eliminated, time_eliminated, raw_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        processed_data["match_id"],
                        processed_data["game_datetime"],
                        processed_data["placement"],
                        processed_data["level"],
                        processed_data["gold_left"],
                        processed_data["last_round"],
                        processed_data["players_eliminated"],
                        processed_data["time_eliminated"],
                        processed_data["raw_data"],
                    ),
                )

            except Exception as e:
                logger.error(f"Error processing match {match_id}: {str(e)}")
                continue

        conn.close()
        logger.info("Successfully processed all matches")

    except Exception as e:
        error_msg = f"Error in fetch_and_store_matches: {str(e)}"
        logger.error(error_msg)
        raise


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda context: logger.error(
        f"Task {context['task_instance'].task_id} failed"
    ),
}

with DAG(
    dag_id="tft_data_ingestion",
    default_args=default_args,
    description="Ingest TFT match data from Riot API",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["tft", "riot", "data-ingestion"],
) as dag:
    daily_update = PythonOperator(
        task_id="fetch_daily_matches",
        python_callable=fetch_and_store_matches,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    historical_load = PythonOperator(
        task_id="fetch_historical_matches",
        python_callable=fetch_and_store_matches,
        op_kwargs={"is_historical": True},
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # Set up task dependencies
    daily_update >> historical_load
