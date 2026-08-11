from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import logging
import sys
from logging.handlers import RotatingFileHandler

load_dotenv()

DB_DEFINITION = Path('table_definition.xml')
# load encoding from env
ENCODING = os.getenv("ENCODING", "utf-8")


class StreamFilter(logging.Filter):
    def filter(self, record):
        if record.levelno in [logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]:
            return True
        else:
            return False


def set_logger(log_name: str, log_level: str = "INFO", log_path: str = "./logs"):
    logger = logging.getLogger(log_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = os.path.join(log_path, f"{log_name}.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=256, encoding=ENCODING)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('[%(name)s] %(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.addFilter(StreamFilter())
    logger.addHandler(stream_handler)
    return logger


LOGGER = set_logger(os.getenv("LOGGER_NAME", "replay"), os.getenv("LOGGER_LEVEL", "INFO"),
                    os.getenv("LOGGER_PATH", "./logs"))


def start_ingestion(file_path: str) -> dict:
    # load the csv files into Tables
    LOGGER.info(f'Loading CSV files from path: {file_path}')
    games_table = _load_csv_into_table(Path(file_path, "games.csv"), ENCODING)
    events_table = _load_csv_into_table(Path(file_path, "events.csv"), ENCODING)
    pitches_table = _load_csv_into_table(Path(file_path, "pitches.csv"), ENCODING)

    # save the tables to the database
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.info(f'Connected to database with connection string: {sql_string}')
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise Exception("No database connection string provided in environment variables.")

    LOGGER.info('Saving tables to database.')
    games_table.to_sql('games', sql_engine, if_exists='replace', index=False)
    events_table.to_sql('events', sql_engine, if_exists='replace', index=False)
    pitches_table.to_sql('pitches', sql_engine, if_exists='replace', index=False)

    LOGGER.info(f'Successfully saved tables to database: games, events, pitches')
    return {
        "games": len(games_table),
        "events": len(events_table),
        "pitches": len(pitches_table)
    }


def _load_csv_into_table(file_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    if not file_path.exists():
        LOGGER.critical("CSV file does not exist.")
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")
    if not DB_DEFINITION.exists():
        LOGGER.critical("Database definition file does not exist.")
        raise FileNotFoundError(f"Database definition file does not exist: {DB_DEFINITION}")

    LOGGER.info(f'Loading CSV file: {file_path}')
    df = pd.read_csv(
        file_path,
        encoding=encoding,
        na_values=["--"],
        keep_default_na=True)
    LOGGER.info(f'Successfully loaded CSV file: {file_path}')
    return df


if __name__ == '__main__':
    result = start_ingestion('../raw_data/')
    print(result)
