from pathlib import Path
import os
from dotenv import load_dotenv
import logging
import sys
from logging.handlers import RotatingFileHandler

import pandas as pd
import sqlalchemy
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

load_dotenv()

# load encoding from env
ENCODING = os.getenv("ENCODING", "utf-8")
FILES = {
    "games": "games.csv",
    "events": "events.csv",
    "pitches": "pitches.csv"
}


class StreamFilter(logging.Filter):
    """Logging filter that allows common stream log levels.

    This filter is added to the stream handler so that regular application
    log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL) are forwarded to stdout.
    """
    def filter(self, record):
        """Decide whether a LogRecord should be emitted to the stream handler.

        Args:
            record: logging.LogRecord instance to evaluate.

        Returns:
            bool: True when the record level is one of the allowed stream levels,
            False otherwise.
        """
        allowed_levels = [logging.INFO, logging.WARNING, logging.ERROR, logging.DEBUG, logging.CRITICAL]
        return record.levelno in allowed_levels


def set_logger(log_name: str, log_level: str = "INFO", log_path: str = "./logs"):
    """Configure and return a logger with file and stream handlers.

    Parameters:
        log_name (str): Base name for the logger and the log file.
        log_level (str): Logging level name (e.g. 'INFO', 'DEBUG').
        log_path (str): Directory where rotating log files are written.

    Returns:
        logging.Logger: Configured logger instance.

    The function ensures the log directory exists, attaches a rotating file
    handler (with encoding from ENV) and a stream handler that writes to stdout.
    The StreamFilter restricts which records are passed to the stream handler.
    """
    logger = logging.getLogger(log_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, log_level.upper()))
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = os.path.join(log_path, f"{log_name}.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 8, backupCount=256, encoding=ENCODING)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('[%(name)s] %(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.addFilter(StreamFilter())
    logger.addHandler(stream_handler)
    return logger


LOGGER = set_logger(os.getenv("LOGGER_NAME", "ingest"), os.getenv("LOGGER_LEVEL", "DEBUG"),
                    os.getenv("LOGGER_PATH", "../logs"))


def start_ingestion(file_path: str) -> dict:
    """Ingest CSV files from a directory into the configured database.

    The function reads CSV files defined in FILES, streams them in chunks
    and writes them to PostgreSQL (connection string taken from
    POSTGRES_DB_CONNECT_STRING environment variable). It returns a dict
    with the number of rows inserted per table.

    Args:
        file_path (str): Directory containing the CSV files.

    Returns:
        dict: Mapping with keys 'games', 'events', 'pitches' and integer counts
        of rows successfully written for each table.

    Raises:
        ValueError: If no database connection string is provided via environment.
        FileNotFoundError: Propagated from CSV loader if files are missing.
    """
    # create a database connection
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.info(f'Connected to database with connection string: {sql_string}')
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise ValueError("No database connection string provided in environment variables.")

    chunk_size = int(os.getenv("CHUNK_SIZE", 100000))
    LOGGER.info(f'Chunk size for database insertion: {chunk_size}')

    table_lines = {}
    for table in FILES.keys():
        table_lines[table] = 0

    # load the csv files into Tables
    LOGGER.info(f'Loading CSV files from path: {file_path}')
    for table, file_name in FILES.items():
        first = True
        file_path_full = Path(file_path, file_name)
        LOGGER.info(f'Loading CSV file: {file_path_full} into table: {table}')
        for chunk in _load_csv_into_table_chunked(file_path_full, encoding=ENCODING, chunk_size=chunk_size):
            try:
                if first:
                    LOGGER.info(f'Inserting first chunk of size {len(chunk)} into table: {table}')
                    chunk.to_sql(table, sql_engine, if_exists='replace', index=False)
                    first = False
                else:
                    LOGGER.info(f'Appending chunk of size {len(chunk)} into table: {table}')
                    chunk.to_sql(table, sql_engine, if_exists='append', index=False)
            except Exception as e:
                LOGGER.error(f'Failed to save chunk to database: {table}. Error: {e}')
            else:
                LOGGER.info(f'Successfully saved chunk to database: {table} with {len(chunk)} records.')
                table_lines[table] += len(chunk)

    return {
        "games": table_lines["games"],
        "events": table_lines["events"],
        "pitches": table_lines["pitches"]
    }


def _load_csv_into_table(file_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    """Load a CSV file fully into a pandas DataFrame.

    Args:
        file_path (Path): Path to the CSV file.
        encoding (str): File encoding to use when reading.

    Returns:
        pd.DataFrame: DataFrame containing the CSV contents.

    Raises:
        FileNotFoundError: If the provided file_path does not exist.
    """
    if not file_path.exists():
        LOGGER.critical("CSV file does not exist.")
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")

    LOGGER.info(f'Loading CSV file: {file_path}')
    df = pd.read_csv(
        file_path,
        encoding=encoding,
        na_values=["--"],
        keep_default_na=True)
    LOGGER.info(f'Successfully loaded CSV file: {file_path}')
    return df


def _load_csv_into_table_chunked(file_path: Path, encoding: str = "utf-8", chunk_size: int = 100000):
    """Yield chunks of a CSV file as pandas DataFrames.

    This generator reads the CSV in streaming fashion using pandas.read_csv
    with the chunksize parameter. Each yielded chunk can be written to the
    database separately to avoid excessive memory usage.

    Args:
        file_path (Path): Path to the CSV file.
        encoding (str): Encoding to use when reading the file.
        chunk_size (int): Number of rows per yielded chunk.

    Yields:
        pd.DataFrame: Next chunk of rows from the CSV file.

    Raises:
        FileNotFoundError: If the provided file_path does not exist.
    """
    if not file_path.exists():
        LOGGER.critical("CSV file does not exist.")
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")

    LOGGER.info(f'Loading CSV file in chunks: {file_path}')
    for chunk in pd.read_csv(
            file_path,
            encoding=encoding,
            na_values=["--"],
            keep_default_na=True,
            chunksize=chunk_size):
        LOGGER.info(f'Loaded chunk of size {len(chunk)} from CSV file: {file_path}')
        yield chunk


class IngestionRequest(BaseModel):
    file_path: str | None = Field(
        default="/app/data/",
        description="Directory containing games.csv, events.csv, and pitches.csv",
    )


class IngestionResponse(BaseModel):
    file_path: str
    games: int
    events: int
    pitches: int


app = FastAPI(title="Ingestion API", version="1.0.0")


@app.get("/health")
def health_check() -> dict[str, str]:
    """Simple health check endpoint for the FastAPI app.

    Returns a small JSON payload indicating the service status. Intended for
    orchestration or uptime checks.

    Returns:
        dict[str, str]: {'status': 'ok'} when the application is alive.
    """
    return {"status": "ok"}


@app.post("/start", response_model=IngestionResponse)
def start_ingestion_endpoint(request: IngestionRequest) -> IngestionResponse:
    """API endpoint to trigger ingestion from a provided directory.

    The endpoint reads the 'file_path' from the request body (or falls back to
    the INGESTION_PATH environment variable) and calls start_ingestion. Errors
    from missing files or configuration are translated into appropriate HTTP
    errors.

    Args:
        request (IngestionRequest): Pydantic model containing the optional
            file_path to ingest from.

    Returns:
        IngestionResponse: Pydantic response model with counts per table and
            the path ingested.

    Raises:
        HTTPException: 404 if CSV files are missing, 400 if configuration is
            invalid (e.g. missing DB connection string).
    """
    ingestion_path = request.file_path or os.getenv("INGESTION_PATH", "../raw_data/")
    try:
        result = start_ingestion(ingestion_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return IngestionResponse(file_path=ingestion_path, **result)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
