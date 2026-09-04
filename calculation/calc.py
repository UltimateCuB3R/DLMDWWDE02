from typing import Any
import os
import re
import pandas as pd
import pandas.errors
import xml.etree.ElementTree as ET
import json
from dotenv import load_dotenv
import sqlalchemy
from pathlib import Path
import logging
import sys
from logging.handlers import RotatingFileHandler

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, RuntimeContext
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.watermark_strategy import WatermarkStrategy

load_dotenv()

DB_DEFINITION = os.getenv("POSTGRES_TABLE_DEFINITION", "/opt/flink/usrlib/table_definition.xml")
ENCODING = os.getenv("ENCODING", "utf-8")


class StreamFilter(logging.Filter):
    def filter(self, record):
        """Filter log records to include only INFO, WARNING, ERROR and CRITICAL levels.

        Args:
            record: A logging.LogRecord instance.

        Returns:
            True if the record level should be emitted to the stream handler, False otherwise.
        """
        if record.levelno in [logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]:
            return True
        else:
            return False


def set_logger(log_name: str, log_level=logging.INFO, log_path: str = "./logs"):
    """Create and configure a logger with file rotation and a filtered stdout stream handler.

    Args:
        log_name: Name of the logger and the logfile ("{log_name}.log").
        log_level: Minimum log level to set on the logger.
        log_path: Directory where log files should be created. Directory is created if missing.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(log_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(log_level)
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


LOGGER = set_logger(os.getenv("LOGGER_NAME", "calculation"), log_path=os.getenv("LOGGER_PATH", "./logs"))


def _load_table_definition(table_name: str) -> dict[str, str]:
    """Load a table column definition map from the XML DB_DEFINITION file.

    Args:
        table_name: Name of the table to look up in the XML definition.

    Returns:
        A dict mapping column names to dtype strings.

    Raises:
        ValueError: If the named table is not found in the definition file.
    """
    tree = ET.parse(Path(DB_DEFINITION))
    root = tree.getroot()
    table_node = root.find(f"./TABLE[@name='{table_name}']")

    if table_node is None:
        LOGGER.critical(f"Table '{table_name}' not found in database definition file: {DB_DEFINITION}")
        raise ValueError(f"Table '{table_name}' not found in database definition file: {DB_DEFINITION}")

    dtypes: dict[str, str] = {}
    for column in table_node.findall("column"):
        column_name = column.attrib.get("name")
        column_dtype = column.attrib.get("dtype")
        if not column_name or not column_dtype:
            continue
        dtypes[column_name] = column_dtype
    return dtypes


def get_sql_engine() -> sqlalchemy.engine.base.Engine:
    """Create and return a SQLAlchemy engine using environment variable POSTGRES_DB_CONNECT_STRING.

    Returns:
        A SQLAlchemy Engine connected using the connection string from POSTGRES_DB_CONNECT_STRING.

    Raises:
        Exception: If the connection string environment variable is not set.
    """
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.debug(f"Database connection string: {sql_string}")
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise Exception("No database connection string provided in environment variables.")
    return sql_engine


def _quote_identifier(identifier: str) -> str:
    """Quote an SQL identifier by wrapping in double quotes and escaping internal quotes.

    Args:
        identifier: The identifier to quote (e.g., table or column name).

    Returns:
        The safely quoted identifier string suitable for embedding in SQL statements.
    """
    return '"' + identifier.replace('"', '""') + '"'


def _get_insert_statement(table_name: str, data: dict[str, Any], key_columns: list[str]) -> tuple[
    sqlalchemy.TextClause, dict[str, Any]]:
    """Build an INSERT ... ON CONFLICT SQL text clause and bind parameters from a data dict.

    Args:
        table_name: Target table name.
        data: Mapping of column names to values for the INSERT.
        key_columns: List of columns that form the conflict target (primary/unique key).

    Returns:
        A tuple (sqlalchemy.TextClause, bind_params) where bind_params is a dict of parameter names to values.

    Raises:
        ValueError: If data is empty or if required key_columns are missing from data.
    """
    if not data:
        raise ValueError("Cannot build insert statement without data.")

    data_columns = list(data.keys())
    missing_key_columns = [column for column in key_columns if column not in data_columns]
    if missing_key_columns:
        raise ValueError(f"Missing key columns in data payload: {missing_key_columns}")

    placeholders: list[str] = []
    bind_params: dict[str, Any] = {}
    for index, column in enumerate(data_columns):
        bind_name = f"v{index}"
        placeholders.append(f":{bind_name}")
        bind_params[bind_name] = data[column]

    quoted_table = _quote_identifier(table_name)
    quoted_columns = ', '.join(_quote_identifier(column) for column in data_columns)
    conflict_columns = ', '.join(_quote_identifier(column) for column in key_columns)
    update_columns = [column for column in data_columns if column not in key_columns]

    if update_columns:
        update_clause = ', '.join(
            f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
            for column in update_columns
        )
        statement_sql = (
            f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({', '.join(placeholders)}) "
            f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
        )
    else:
        statement_sql = (
            f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({', '.join(placeholders)}) "
            f"ON CONFLICT ({conflict_columns}) DO NOTHING"
        )

    return sqlalchemy.sql.text(statement_sql), bind_params


def _build_create_table_statement(table_name: str,
                                  table_definition: dict[str, str],
                                  primary_key_columns: list[str]) -> sqlalchemy.TextClause:
    """Construct a CREATE TABLE IF NOT EXISTS statement from a lightweight table definition.

    Args:
        table_name: Name of the table to create.
        table_definition: Mapping of column names to lightweight dtype strings (e.g., 'int64', 'str').
        primary_key_columns: List of column names to use as the PRIMARY KEY.

    Returns:
        sqlalchemy.TextClause with the CREATE TABLE statement.

    Raises:
        ValueError: If table_definition is empty, primary key columns are missing, or an unsupported dtype is encountered.
    """
    sql_type_mapping = {
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "str": "TEXT",
        "bool": "BOOLEAN",
        "datetime": "TIMESTAMP",
    }

    if not table_definition:
        raise ValueError(f"Table definition for '{table_name}' is empty.")

    missing_pk_columns = [column for column in primary_key_columns if column not in table_definition]
    if missing_pk_columns:
        raise ValueError(f"Primary key columns not found in definition for '{table_name}': {missing_pk_columns}")

    column_definitions: list[str] = []
    for column_name, dtype in table_definition.items():
        if dtype not in sql_type_mapping:
            raise ValueError(f"Unsupported data type '{dtype}' for column '{column_name}' in table '{table_name}'")
        quoted_column = _quote_identifier(column_name)
        column_definitions.append(f"{quoted_column} {sql_type_mapping[dtype]}")

    primary_key_sql = ", ".join(_quote_identifier(column) for column in primary_key_columns)
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} "
        f"({', '.join(column_definitions)}, PRIMARY KEY ({primary_key_sql}))"
    )
    return sqlalchemy.sql.text(create_sql)


def _get_delete_statement(table_name: str, game_id: str) -> tuple[sqlalchemy.TextClause, dict[str, Any]]:
    """Build a parameterized DELETE statement for a given game id.

    Args:
        table_name: Table from which to delete rows.
        game_id: Identifier of the game to delete.

    Returns:
        A tuple (sqlalchemy.TextClause, params) where params is a dict containing 'game_id'.

    Raises:
        ValueError: If game_id is empty or falsy.
    """
    if not game_id:
        raise ValueError("Cannot build delete statement without game_id.")

    statement_sql = f"DELETE FROM public.{_quote_identifier(table_name)} WHERE {_quote_identifier('game')} = CAST(:game_id AS TEXT)"
    return sqlalchemy.sql.text(statement_sql), {"game_id": game_id}


class Calculation:
    game_state: dict
    inning_state: dict
    pitcher_state: dict
    db_con: sqlalchemy.Engine
    log: logging.Logger

    def __init__(self, db_con):
        """Initialize Calculation instance by loading table definitions and preparing DB tables.

        Args:
            db_con: SQLAlchemy Engine or connection that will be used for database operations.

        The constructor sets up a dedicated logger, loads XML table definitions for game_state,
        innings and pitchers, stores the provided DB connection and ensures required tables exist.
        """
        self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                              log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
        self._def_game_state: dict[str, str] = _load_table_definition("game_state")
        self._def_innings: dict[str, str] = _load_table_definition("innings")
        self._def_pitchers: dict[str, str] = _load_table_definition("pitchers")
        self.db_con = db_con
        self._init_tables()

    def _init_tables(self):
        """Ensure in-memory state structures are initialized and that required DB tables exist.

        This method inspects the database, initializes empty in-memory dicts for game_state,
        inning_state and pitcher_state based on the loaded table definitions, and creates the
        corresponding tables if they are missing.
        """
        self.log.info('Initializing Tables...')
        db_inspect = sqlalchemy.inspect(self.db_con)

        self.game_state = {key: None for key in self._def_game_state.keys()}
        self.log.debug(f'Game state after init: {self.game_state}')
        if not db_inspect.has_table("game_state"):
            create_stmt = _build_create_table_statement("game_state", self._def_game_state, ["game"])
            with self.db_con.begin() as con:
                sql_result = con.execute(create_stmt)
            self.log.debug(f'CREATE TABLE result for game_state: {sql_result}')

        self.inning_state = {key: None for key in self._def_innings.keys()}
        self.log.debug(f'Inning state after init: {self.inning_state}')
        if not db_inspect.has_table("innings"):
            create_stmt = _build_create_table_statement("innings", self._def_innings, ["game", "inning"])
            with self.db_con.begin() as con:
                sql_result = con.execute(create_stmt)
            self.log.debug(f'CREATE TABLE result for innings: {sql_result}')

        self.pitcher_state = {key: None for key in self._def_pitchers.keys()}
        self.log.debug(f'Pitcher state after init: {self.pitcher_state}')
        if not db_inspect.has_table("pitchers"):
            create_stmt = _build_create_table_statement("pitchers", self._def_pitchers, ["game", "pitcher"])
            with self.db_con.begin() as con:
                sql_result = con.execute(create_stmt)
            self.log.debug(f'CREATE TABLE result for pitchers: {sql_result}')

        self.log.info('All Tables initialized: game_state, innings, pitchers')

    def _read_game_state(self, game_id: str):
        """Load game_state row for a given game_id into the in-memory self.game_state dict.

        Args:
            game_id: The game identifier to query.

        Behavior:
            - If exactly one row is found, populate self.game_state with that row.
            - If no rows are found, initialize self.game_state with default None values.
            - If multiple rows are found, log a critical error and raise ValueError.
        """
        game_df = pd.read_sql_query(f"SELECT * FROM public.game_state WHERE game = '{game_id}'", self.db_con)
        # determine length of dataframe, if 1 then read first row as dict, if 0 then create new row with default values, if >1 then raise error
        if len(game_df) == 1:
            self.game_state = game_df.iloc[0].to_dict()
            self.log.debug(f"Read game state for game_id {game_id}: {self.game_state}")
        elif len(game_df) == 0:
            self.game_state = {key: None for key in self._def_game_state.keys()}
            self.log.debug(
                f"No game state found for game_id {game_id}, initialized with default values: {self.game_state}")
        else:
            self.log.critical(f"Unexpected number of rows in game_state for game_id {game_id}: {len(game_df)}")
            raise ValueError("Unexpected number of rows in game_state")

    def _read_game_innings(self, game_id: str, inning: str):
        """Load a specific inning row into in-memory self.inning_state.

        Args:
            game_id: The game identifier.
            inning: The inning identifier (e.g., '1' or '1_bottom').

        Behavior:
            - Populates self.inning_state from the DB if exactly one row is found.
            - Initializes defaults if no row is found.
            - Raises ValueError if more than one row is returned.
        """
        inning_df = pd.read_sql_query(
            f"SELECT * FROM public.innings WHERE game = '{game_id}' AND inning = '{inning}'",
            self.db_con)
        if len(inning_df) == 1:
            self.inning_state = inning_df.iloc[0].to_dict()
            self.log.debug(f"Read inning state for game_id {game_id}, inning {inning}: {self.inning_state}")
        elif len(inning_df) == 0:
            self.inning_state = {key: None for key in self._def_innings.keys()}
            self.log.debug(
                f"No inning state found for game_id {game_id}, inning {inning}, initialized with default values: {self.inning_state}")
        else:
            self.log.critical(
                f"Unexpected number of rows in innings for game_id {game_id}, inning {inning}: {len(inning_df)}")
            raise ValueError("Unexpected number of rows in innings")

    def _read_game_pitchers(self, game_id: str, pitcher: str):
        """Load a specific pitcher row into in-memory self.pitcher_state.

        Args:
            game_id: The game identifier.
            pitcher: The pitcher identifier.

        Behavior:
            - Populates self.pitcher_state if exactly one row exists.
            - Initializes defaults if no row exists.
            - Raises ValueError when multiple rows are found.
        """
        pitcher_df = pd.read_sql_query(
            f"SELECT * FROM public.pitchers WHERE game = '{game_id}' AND pitcher = '{pitcher}'", self.db_con)
        if len(pitcher_df) == 1:
            self.pitcher_state = pitcher_df.iloc[0].to_dict()
            self.log.debug(f"Read pitcher state for game_id {game_id}, pitcher {pitcher}: {self.pitcher_state}")
        elif len(pitcher_df) == 0:
            self.pitcher_state = {key: None for key in self._def_pitchers.keys()}
            self.log.debug(
                f"No pitcher state found for game_id {game_id}, pitcher {pitcher}, initialized with default values: {self.pitcher_state}")
        else:
            self.log.critical(
                f"Unexpected number of rows in pitchers for game_id {game_id}, pitcher {pitcher}: {len(pitcher_df)}")
            raise ValueError("Unexpected number of rows in pitchers")

    def _delete_game_data(self, game_id: str):
        """Remove all rows related to a game across game_state, innings and pitchers tables.

        Args:
            game_id: Identifier of the game to delete.

        Behavior:
            Executes parameterized DELETE statements for the three managed tables.
        """
        self.log.info(f"Deleting all data for game_id {game_id}")
        for table in ["game_state", "innings", "pitchers"]:
            delete_stmt, params = _get_delete_statement(table, game_id)
            with self.db_con.connect() as con:
                sql_result = con.execute(delete_stmt, params)
            self.log.debug(f'DELETE result for {table} where game={game_id}: {sql_result}')

    def receive_event(self, event: dict[str, Any]):
        """Top-level entry to process an incoming game event and return the updated state.

        Args:
            event: Dict representing a single input event. Expected keys include 'game-id' and 'event-type'.

        Behavior:
            - Loads current game state from DB.
            - Dispatches to specific processors based on event-type (game, inning, pitch, event).
            - Catches ValueError raised during processing and logs critically.

        Returns:
            A dictionary containing the updated 'game-state', 'inning-state' and 'pitcher-state'.
        """
        game_id = str(event["game-id"])
        # read game state and innings via sqlalchemy
        self._read_game_state(game_id)

        try:
            self.log.info(f'Processing event-type {event["event-type"]}')
            match event["event-type"]:
                case "game-start" | "game-end":
                    self._process_game(event)
                case "inning-start" | "inning-end":
                    self._read_game_innings(game_id, event["inning"])
                    self._process_inning(event)
                case "pitch":
                    self._read_game_pitchers(game_id, event["pitcher"])
                    self._process_pitch(event)
                case "event":
                    self._process_event(event)
        except ValueError:
            self.log.critical(f"Error processing event: {event}")
        result_state = {
            "game-state": self.game_state,
            "inning-state": self.inning_state,
            "pitcher-state": self.pitcher_state
        }
        self.log.info(f"Returning result state: {result_state}")
        return result_state

    def _process_game(self, event: dict[str, Any]):
        """Process game-start and game-end events, updating in-memory game_state accordingly.

        Args:
            event: Event dict containing at least 'game-id' and 'event-type'. For game-start a variety
                   of static fields (home, away, stadium, etc.) may be present. For game-end it should
                   contain final scores and duration.

        Behavior:
            - On 'game-start' initializes game_state from event data or defaults and may delete
              any previous data for a restarted game.
            - On 'game-end' verifies final scores against calculated ones, updates final fields and
              marks game-running False.

        Raises:
            ValueError: If the event-type is unexpected or if a restart is detected while the game
                        is already marked as running.
        """
        game_id = str(event["game-id"])
        if event["event-type"] == "game-start":
            self.log.info(f"Starting game: {game_id}")
            # check if game is already running
            if str(self.game_state["game"]) == game_id:
                self.log.warning(f"Game with ID {game_id} already started!")
                if self.game_state["game-running"]:
                    raise ValueError(f"Game with ID {game_id} already started!")
                else:
                    self.log.warning(
                        f"Game with ID {game_id} was previously ended, restarting and cleaning up database...")
                    self._delete_game_data(game_id)

            # create new game entry with this game id
            # set all static values: game-id, away, home, stadium, date, location, umpires
            # initialize the dynamic values: away-score, home-score, duration
            # set game_running to TRUE
            for key in self._def_game_state.keys():
                if key in event.keys():
                    self.game_state[key] = event[key]
                else:
                    if key == "game":
                        self.game_state[key] = game_id
                    elif key == "away-score" or key == "home-score" or key == "out":
                        self.game_state[key] = 0
                    elif key == "game-running":
                        self.game_state[key] = True
                    else:
                        self.game_state[key] = None
                if key == "location" or key == "stadium":
                    # strip all tabs, newlines and multiple spaces from location and stadium
                    self.game_state[key] = " ".join(event[key].split()) if event[key] else ""

        elif event["event-type"] == "game-end":
            self.log.info(f"Game ended: {game_id}")
            # check if calculated scores match final game scores
            final_away_score = self._to_int(event["away-score"])
            final_home_score = self._to_int(event["home-score"])
            if self.game_state["away-score"] != final_away_score or self.game_state["home-score"] != final_home_score:
                self.log.warning(
                    f"Calculated scores ({self.game_state['away-score']}, {self.game_state['home-score']}) do not match final scores ({final_away_score}, {final_home_score}) for game {game_id}")

            # set the dynamic values: away-score, home-score, duration
            # set game_running to FALSE
            self.game_state["away-score"] = final_away_score
            self.game_state["home-score"] = final_home_score
            self.game_state["duration"] = event["duration"]
            self.game_state["game-running"] = False

        else:
            self.log.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_inning(self, event: dict[str, Any]):
        """Handle inning-start and inning-end events, updating inning_state and related game_state fields.

        Args:
            event: Event dict with keys such as 'game-id', 'event-type', 'inning', scores, etc.

        Behavior:
            - On 'inning-start' initializes inning_state fields from the event or defaults and sets game_state['inning'].
            - On 'inning-end' updates inning scores, marks inning_running False and resets transient game_state fields
              such as outs, count and base runners.

        Raises:
            ValueError: If event-type is not recognized.
        """
        game_id = str(event["game-id"])
        if event["event-type"] == "inning-start":
            self.log.info(f"Inning started: {event['inning']} in game {game_id}")
            # create a new half-inning entry for this game id
            # static: game-id, batting-team, pitching-team, inning
            # initialize the dynamic values: away-score, home-score
            for key in self._def_innings.keys():
                if key in event.keys():
                    self.inning_state[key] = event[key]
                else:
                    if key == "game":
                        self.inning_state[key] = game_id
                    elif key == "away-score" or key == "home-score":
                        self.inning_state[key] = 0
                    elif key == "inning-running":
                        self.inning_state[key] = True
                    else:
                        self.inning_state[key] = None
            self.game_state["inning"] = event["inning"]

        elif event["event-type"] == "inning-end":
            self.log.info(f"Inning ended: {event['inning']} in game {game_id}")
            # set the dynamic values: away-score, home-score
            self.inning_state["away-score"] = self._to_int(event["away-score"])
            self.inning_state["home-score"] = self._to_int(event["home-score"])
            self.inning_state["inning-running"] = False

            self.game_state["out"] = 0
            self.game_state["count"] = ''
            self.game_state["runner-1"] = ''
            self.game_state["runner-2"] = ''
            self.game_state["runner-3"] = ''
            # LOGGER.debug(f'Game data in this inning: {self.game_state}')
        else:
            self.log.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_pitch(self, event: dict[str, Any]):
        """Process a pitch event, updating count, pitcher and base-runner fields.

        Args:
            event: Event dict with keys such as 'game-id','pitch-id','pitch','pitch-type','pitch-speed',
                   'pitch-location', and 'play-bases'.

        Behavior:
            - Initializes count to 0-0 for the first pitch of a plate appearance.
            - Updates balls/strikes based on heuristics applied to the 'pitch' text.
            - Parses pitch-location strings to extract numeric coordinates when present.
            - Updates runner-1/2/3 based on 'play-bases'.
            - Maintains per-pitcher pitch counts in self.pitcher_state.

        Raises:
            ValueError: If event-type is not 'pitch'.
        """
        game_id = str(event["game-id"])
        if event["event-type"] == "pitch":
            self.log.info(f"Pitch: {event['pitch-id']} for event {event['event-id']} of game {game_id}")
            # create new pitch entry for game id and event id
            # values: pitcher, pitch-id, pitch, pitch-type, pitch-speed, pitch-location, pitching-team
            # calculation needed: pitch -> call (strike/ball)
            if event["pitch-id"] == 1:
                self.game_state["count"] = "0-0"

            balls, strikes = self.game_state["count"].split("-") if self.game_state["count"] else (0, 0)
            balls = self._to_int(balls)
            strikes = self._to_int(strikes)

            pitch = str(event["pitch"]).lower()
            if "strike" in pitch or "bunted foul" in pitch:
                strikes += 1
            elif "ball" in pitch and not "foul" in pitch and not "hit by" in pitch:
                balls += 1
            elif "foul" in pitch and not "foul out" in pitch:
                if strikes < 2:
                    strikes += 1
            self.game_state["count"] = f"{balls}-{strikes}"
            self.game_state["pitcher"] = event["pitcher"]
            self.game_state["pitch-type"] = event["pitch-type"]
            self.game_state["pitch-speed"] = event["pitch-speed"]

            if event["pitch-location"] is not None:
                # extract pitch location from string like "top: 123.45px; right: 67.89px;"
                match = re.search(
                    r"top:\s*([0-9]+(?:\.[0-9]+)?)px;\s*right:\s*([0-9]+(?:\.[0-9]+)?)px;",
                    str(event["pitch-location"]),
                )
                if match:
                    self.game_state["pitch_y"] = float(match.group(1))
                    self.game_state["pitch_x"] = float(match.group(2))

            if event["play-bases"] is not None:
                # set all runner on bases
                self.game_state["runner-1"] = "X" if "1" in str(event["play-bases"]) else ""
                self.game_state["runner-2"] = "X" if "2" in str(event["play-bases"]) else ""
                self.game_state["runner-3"] = "X" if "3" in str(event["play-bases"]) else ""

            # update pitcher state with pitch count
            if self.pitcher_state.get("pitcher") is None:
                self.pitcher_state["pitcher"] = event["pitcher"]
                self.pitcher_state["game"] = event["game-id"]
                self.pitcher_state["team"] = event["pitching-team"]
                self.pitcher_state["pitches"] = 1
            else:
                self.pitcher_state["pitches"] = self._to_int(self.pitcher_state["pitches"]) + 1
        else:
            self.log.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_event(self, event: dict[str, Any]):
        """Handle generic in-play events, updating scores, outs and potential pitcher attribution.

        Args:
            event: Event dict containing 'away-score','home-score','event-text', 'event-id', and 'inning'.

        Behavior:
            - Updates game_state scores from the event payload.
            - Parses event-text to increment outs for 'out' and 'double play', and tries to extract pitcher name
              when the text contains the word 'pitching'.

        Raises:
            ValueError: If event-type is not 'event'.
        """
        game_id = str(event["game-id"])
        if event["event-type"] == "event":
            self.log.info(f"Event: {event['event-id']} in inning {event['inning']} of game {game_id}")
            # create new event entry for game id and event id
            # values: batting-team, pitching-team, inning, event-text, away-score, home-score
            # calculation needed: event-text -> number of outs; runner on bases

            self.game_state["away-score"] = self._to_int(event["away-score"])
            self.game_state["home-score"] = self._to_int(event["home-score"])
            event_text = "" if pd.isna(event["event-text"]) else str(event["event-text"]).lower()
            if "double play" in event_text:
                self.game_state["out"] += 2
            elif "out" in event_text:
                self.game_state["out"] += 1
            if "pitching" in event_text:
                self.game_state["pitcher"] = event["event-text"].split("pitching")[0].strip()
            self.log.info(f'current outs: {self.game_state["out"]}')
        else:
            self.log.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    @staticmethod
    def _to_int(value: Any) -> int:
        """Safely convert a value to int, returning 0 for invalid input.

        Args:
            value: Any value that may be cast to int.

        Returns:
            Integer representation or 0 when conversion fails.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0


class PostgresSinkFunction(MapFunction):
    def __init__(self):
        """Initialize the sink function instance. Connections/loggers created in open()."""
        self.log = None
        self.conn = None

    def open(self, runtime_context: RuntimeContext):
        """Flink lifecycle method called once to initialize resources like logger and DB connection.

        Args:
            runtime_context: Flink runtime context (unused but provided by the framework).
        """
        if self.log:
            self.log.info("Logger for PostgresSinkFunction already initialized")
        else:
            self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                                  log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
            self.log.info("Initialized logger for PostgresSinkFunction")
        if self.conn:
            self.log.info("PostgreSQL connection for PostgresSinkFunction already established")
        else:
            self.log.info("Connection to PostgreSQL for Sink established")
            self.conn = get_sql_engine().connect().execution_options(isolation_level="AUTOCOMMIT")

    def map(self, value):
        """Consume a serialized state payload (JSON), and persist game, inning and pitcher state to Postgres.

        Args:
            value: JSON-encoded string containing keys 'game-state','inning-state','pitcher-state'.

        Returns:
            None

        Raises:
            ConnectionError: If the DB connection has not been established via open().
        """
        state = json.loads(value)
        game_state = state.get("game-state")
        inning_state = state.get("inning-state")
        pitcher_state = state.get("pitcher-state")
        if not self.conn:
            self.log.critical("PostgreSQL connection is not established.")
            raise ConnectionError("PostgreSQL connection is not established.")
        self.log.info("Saving state to PostgreSQL")
        if game_state.get("game"):
            insert, values = _get_insert_statement("game_state", game_state, ["game"])
            self.log.debug(f"Executing insert statement: {insert} with values: {values}")
            self.conn.execute(insert, values)
            self.log.info(f"Inserted/Updated game_state for game: {game_state.get('game')}")
        if inning_state.get("game") and inning_state.get("inning"):
            insert, values = _get_insert_statement("innings", inning_state, ["game", "inning"])
            self.log.debug(f"Executing insert statement: {insert} with values: {values}")
            self.conn.execute(insert, values)
            self.log.info(
                f"Inserted/Updated innings for game: {inning_state.get('game')}, inning: {inning_state.get('inning')}")
        if pitcher_state.get("game") and pitcher_state.get("pitcher"):
            insert, values = _get_insert_statement("pitchers", pitcher_state, ["game", "pitcher"])
            self.log.debug(f"Executing insert statement: {insert} with values: {values}")
            self.conn.execute(insert, values)
            self.log.info(
                f"Inserted/Updated pitchers for game: {pitcher_state.get('game')}, pitcher: {pitcher_state.get('pitcher')}")
        return None

    def close(self):
        """Flink lifecycle close: log and release DB connection if present."""
        if self.log:
            self.log.info("Closing connection to PostgreSQL for Sink")
        if self.conn:
            self.conn.close()
            self.conn = None


class BusinessLogicMapper(MapFunction):
    def __init__(self):
        """Mapper that applies business logic to incoming events using Calculation.

        Resources (logger, Calculation) are created in open()."""
        self.log = None
        self.game_calc = None

    def open(self, runtime_context: RuntimeContext):
        """Flink lifecycle hook to initialize logger and Calculation helper.

        Args:
            runtime_context: Flink RuntimeContext (unused but provided by framework).
        """
        if self.log:
            self.log.info("Logger for BusinessLogicMapper already initialized")
        else:
            self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                                  log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
            self.log.info("Initialized logger for BusinessLogicMapper")
        if self.game_calc:
            self.log.info("Calculation object for BusinessLogicMapper already initialized")
        else:
            self.game_calc = Calculation(get_sql_engine())
            self.log.info("Initialized Calculation object for BusinessLogicMapper")
        self.log.info("BusinessLogicMapper opened successfully")

    def map(self, value):
        """Flink map method: deserialize event JSON, run business logic, and serialize resulting state.

        Args:
            value: JSON-encoded string representing an input event.

        Returns:
            JSON string of the current combined state (game, inning, pitcher), or null-state on DB error.
        """
        event = json.loads(value)
        self.log.info(f"Received message: {event}")
        try:
            result = self.game_calc.receive_event(event)
            self.log.debug(f"Received total state: {result}")
        except pandas.errors.DatabaseError as e:
            self.log.critical(f"DatabaseError processing event: {e}")
            result = {
                "game-state": None,
                "inning-state": None,
                "pitcher-state": None
            }

        return json.dumps(result)

    def close(self):
        """Flink close lifecycle method: release Calculation reference and log shutdown."""
        if self.log:
            self.log.info("Closing BusinessLogicMapper")
        if self.game_calc:
            self.game_calc = None


class EventTimestampAssigner(TimestampAssigner):
    log: logging.Logger

    def extract_timestamp(self, value, record_timestamp):
        """Extract an integer event timestamp from the JSON payload for Flink record timestamping.

        Args:
            value: Serialized event payload (expected JSON string).
            record_timestamp: Fallback timestamp provided by the runtime.

        Returns:
            Integer timestamp to use for the event. Falls back to record_timestamp on parse error.
        """
        event = json.loads(value)
        if self.log:
            self.log.debug("Using existing logger for timestamp extraction")
        else:
            self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                                  log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
            self.log.debug("Creating new logger for timestamp extraction")
        try:
            this_timestamp = int(event.get("event-timestamp", record_timestamp))
        except (ValueError, TypeError):
            self.log.warning(f"Invalid timestamp in event: {event}")
            this_timestamp = record_timestamp
        self.log.info(f"Extracted timestamp: {this_timestamp} from event: {event}")
        return this_timestamp


def main():
    """Entry point to set up and execute the PyFlink streaming job.

    Behavior:
        - Configures the StreamExecutionEnvironment for streaming with checkpointing.
        - Builds a Kafka source and assigns an EventTimestampAssigner.
        - Applies BusinessLogicMapper and PostgresSinkFunction to persist computed state.
        - Prints the processed stream and starts execution.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    LOGGER.info("Setting up Flink Streaming Job for Game Events Calculation")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(30000)

    LOGGER.info(
        f"Connecting to Kafka at {os.getenv('KAFKA_HOST', 'kafka')}:{os.getenv('KAFKA_PORT', '9092')} and subscribing to topic '{os.getenv('KAFKA_TOPIC', 'game-events')}'")
    kafka_source = KafkaSource.builder()
    LOGGER.debug("Kafka source builder initialized")
    kafka_source = kafka_source.set_bootstrap_servers(
        ":".join([os.getenv('KAFKA_HOST', 'kafka'), os.getenv('KAFKA_PORT', '9092')]))
    LOGGER.debug("Kafka source bootstrap servers set")
    kafka_source = kafka_source.set_topics(os.getenv('KAFKA_TOPIC', 'game-events'))
    LOGGER.debug("Kafka source topics set")
    kafka_source = kafka_source.set_group_id("flink-consumer")
    LOGGER.debug("Kafka source group_id set")
    kafka_source = kafka_source.set_starting_offsets(KafkaOffsetsInitializer.earliest())
    LOGGER.debug("Kafka source starting offsets set to earliest")
    kafka_source = kafka_source.set_value_only_deserializer(SimpleStringSchema())
    LOGGER.debug("Kafka source value deserializer set to SimpleStringSchema")
    kafka_source = kafka_source.build()
    LOGGER.debug("Kafka source build")

    LOGGER.info("Kafka source initialized")
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            EventTimestampAssigner()),
        source_name=os.getenv('KAFKA_TOPIC', 'game-events')
    )

    LOGGER.info("Mapping stream to business logic and Postgres sink")
    processed_stream = stream.map(BusinessLogicMapper(),
                                  output_type=Types.PICKLED_BYTE_ARRAY()
                                  ).map(PostgresSinkFunction(),
                                        output_type=Types.PICKLED_BYTE_ARRAY())

    LOGGER.info('Showing alerts in the console')
    processed_stream.print()

    LOGGER.info("Executing Flink Streaming Job for Game Events Calculation")
    env.execute("game-events-calculation")


if __name__ == "__main__":
    main()
