from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

load_dotenv()

DB_DEFINITION = Path('table_definition.xml')
# load encoding from env
ENCODING = os.getenv("ENCODING", "utf-8")


def start_ingestion(file_path: str) -> dict:
    # load the csv files into Tables
    games_table = _load_csv_into_table(Path(file_path, "games.csv"), ENCODING)
    events_table = _load_csv_into_table(Path(file_path, "events.csv"), ENCODING)
    pitches_table = _load_csv_into_table(Path(file_path, "pitches.csv"), ENCODING)

    # save the tables to the database
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
    else:
        raise Exception("No database connection string provided in environment variables.")

    games_rows = games_table.to_sql('games', sql_engine, if_exists='replace', index=False)
    events_rows = events_table.to_sql('events', sql_engine, if_exists='replace', index=False)
    pitches_rows = pitches_table.to_sql('pitches', sql_engine, if_exists='replace', index=False)

    return {
        "games": games_rows,
        "events": events_rows,
        "pitches": pitches_rows
    }


@dataclass(frozen=True)
class PdTable:
    table_name: str
    df: pd.DataFrame
    # TODO: remove all usages of this, then delete


def _load_csv_into_table(file_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")
    if not DB_DEFINITION.exists():
        raise FileNotFoundError(f"Database definition file does not exist: {DB_DEFINITION}")

    return pd.read_csv(
        file_path,
        encoding=encoding,
        na_values=["--"],
        keep_default_na=True,
    )


if __name__ == '__main__':
    result = start_ingestion('../raw_data/')
    print(result)
