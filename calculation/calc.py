from typing import Any
import os
import re
import pandas as pd
import pandas.errors
from kafka import KafkaConsumer
import xml.etree.ElementTree as ET
import json
from dotenv import load_dotenv
import sqlalchemy
from pathlib import Path
import logging
import sys
from logging.handlers import RotatingFileHandler

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.functions import MapFunction, SinkFunction
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
        if record.levelno in [logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]:
            return True
        else:
            return False


def set_logger(log_name: str, log_level=logging.DEBUG, log_path: str = "./logs"):
    logger = logging.getLogger(log_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(log_level)
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


LOGGER = set_logger(os.getenv("LOGGER_NAME", "calculation"), log_path=os.getenv("LOGGER_PATH", "./logs"))


def _load_table_definition(table_name: str) -> dict[str, str]:
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


def _table_definition_to_types(table_definition: dict[str, str]) -> Types:
    type_mapping = {
        "int64": Types.INT(),
        "float64": Types.FLOAT(),
        "str": Types.STRING(),
        "bool": Types.BOOLEAN(),
        "datetime": Types.SQL_TIMESTAMP(),
    }

    field_types = []
    for column_name, dtype in table_definition.items():
        if dtype not in type_mapping:
            LOGGER.critical(f"Unsupported data type '{dtype}' for column '{column_name}'")
            raise ValueError(f"Unsupported data type '{dtype}' for column '{column_name}'")
        field_types.append(type_mapping[dtype])

    return Types.ROW_NAMED(list(table_definition.keys()), field_types)


def set_consumer(server: str, port: int, topic: str):
    return KafkaConsumer(topic,
                         bootstrap_servers=f'{server}:{port}',
                         key_deserializer=lambda k: str(k).encode(ENCODING),
                         value_deserializer=lambda v: json.loads(v.decode(ENCODING)))


def start_calculation():
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.debug(f"Database connection string: {sql_string}")
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise Exception("No database connection string provided in environment variables.")

    game_calc = Calculation(sql_engine)
    consumer = set_consumer(server=os.getenv('KAFKA_HOST', 'localhost'),
                            port=int(os.getenv('KAFKA_PORT', 9092)),
                            topic=os.getenv('KAFKA_TOPIC', 'game-events'))
    # consumer.subscribe(['game-events'])
    for msg in consumer:
        LOGGER.info(f"Received message: {msg}")
        try:
            total_state = game_calc.receive_event(msg.value)
            LOGGER.debug(f"Received total state: {total_state}")
        except pandas.errors.DatabaseError as e:
            LOGGER.critical(f"DatabaseError processing event: {e}")


class Calculation:
    game_state: dict
    inning_state: dict
    pitcher_state: dict
    db_con: sqlalchemy.Engine
    log: logging.Logger

    def __init__(self, db_con):
        self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                              log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
        self._def_game_state: dict[str, str] = _load_table_definition("game_state")
        self._def_innings: dict[str, str] = _load_table_definition("innings")
        self._def_pitchers: dict[str, str] = _load_table_definition("pitchers")
        self.db_con = db_con
        self._init_tables()

    def _init_tables(self):
        self.log.info('Initializing Tables...')
        db_inspect = sqlalchemy.inspect(self.db_con)

        self.game_state = {key: None for key in self._def_game_state.keys()}
        self.log.debug(f'Game state after init: {self.game_state}')
        if not db_inspect.has_table("game_state"):
            game_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_game_state.items()})
            sql_result = game_df.to_sql('game_state', self.db_con, if_exists='replace', index=False)
            with self.db_con.connect() as con:
                con.execute(sqlalchemy.text('ALTER TABLE game_state ADD PRIMARY KEY (game);'))
            self.log.debug(f'SQL result of game_state initialization: {sql_result}')

        self.inning_state = {key: None for key in self._def_innings.keys()}
        self.log.debug(f'Inning state after init: {self.inning_state}')
        if not db_inspect.has_table("innings"):
            innings_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_innings.items()})
            sql_result = innings_df.to_sql('innings', self.db_con, if_exists='replace', index=False)
            with self.db_con.connect() as con:
                con.execute(sqlalchemy.text('ALTER TABLE innings ADD PRIMARY KEY (game, inning);'))
            self.log.debug(f'SQL result of innings initialization: {sql_result}')

        self.pitcher_state = {key: None for key in self._def_pitchers.keys()}
        self.log.debug(f'Pitcher state after init: {self.pitcher_state}')
        if not db_inspect.has_table("pitchers"):
            pitchers_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_pitchers.items()})
            sql_result = pitchers_df.to_sql('pitchers', self.db_con, if_exists='replace', index=False)
            with self.db_con.connect() as con:
                con.execute(sqlalchemy.text('ALTER TABLE pitchers ADD PRIMARY KEY (game, pitcher);'))
            self.log.debug(f'SQL result of pitchers initialization: {sql_result}')

        self.log.info('Tables initialized.')

    def _read_game_state(self, game_id):
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

    def _read_game_innings(self, game_id, inning):
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

    def _read_game_pitchers(self, game_id, pitcher):
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

    def _save_game_state(self):
        self.log.debug(f'Saving game state: {self.game_state}')
        game_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_game_state.items()})
        game_df.loc[0] = self.game_state
        self.log.debug(f'Game state DataFrame to save: {game_df}')
        result = game_df.to_sql('game_state', self.db_con, if_exists='replace', index=False)
        self.log.debug(f'result of to_sql (game_state): {result}')

    def _save_game_innings(self):
        self.log.debug(f'Saving innings state: {self.inning_state}')
        inning_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_innings.items()})
        inning_df.loc[0] = self.inning_state
        self.log.debug(f'Inning state DataFrame to save: {inning_df}')
        result = inning_df.to_sql('innings', self.db_con, if_exists='replace', index=False)
        self.log.debug(f'result of to_sql (innings): {result}')

    def _save_game_pitchers(self):
        self.log.debug(f'Saving pitchers state: {self.pitcher_state}')
        pitcher_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self._def_pitchers.items()})
        pitcher_df.loc[0] = self.pitcher_state
        self.log.debug(f'Pitcher state DataFrame to save: {pitcher_df}')
        result = pitcher_df.to_sql('pitchers', self.db_con, if_exists='replace', index=False)
        self.log.debug(f'result of to_sql (pitchers): {result}')

    def receive_event(self, event: dict[str, Any]):
        game_id = event["game-id"]
        # read game state and innings via sqlalchemy
        self._read_game_state(game_id)

        try:
            self.log.info(f'Processing event-type {event["event-type"]}')
            match event["event-type"]:
                case "game-start" | "game-end":
                    self._process_game(event)
                    # self._save_game_state()
                case "inning-start" | "inning-end":
                    self._read_game_innings(game_id, event["inning"])
                    self._process_inning(event)
                    # self._save_game_state()
                    # self._save_game_innings()
                case "pitch":
                    self._read_game_pitchers(game_id, event["pitcher"])
                    self._process_pitch(event)
                    # self._save_game_state()
                    # self._save_game_pitchers()  # Pitch counter per Pitcher per Game
                case "event":
                    self._process_event(event)
                    # self._save_game_state()
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
        if event["event-type"] == "game-start":
            self.log.info(f"Game started: {event['game-id']}")
            # check if game is already running
            if self.game_state["game"] == event["game-id"]:
                self.log.critical(f"Game with ID {event['game-id']} already started!")
                raise ValueError(f"Game with ID {event['game-id']} already started!")

            # create new game entry with this game id
            # set all static values: game-id, away, home, stadium, date, location, umpires
            # initialize the dynamic values: away-score, home-score, duration
            # set game_running to TRUE
            for key in self._def_game_state.keys():
                if key in event.keys():
                    self.game_state[key] = event[key]
                else:
                    if key == "game":
                        self.game_state[key] = event["game-id"]
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
            self.log.info(f"Game ended: {event['game-id']}")
            # check if calculated scores match final game scores
            final_away_score = self._to_int(event["away-score"])
            final_home_score = self._to_int(event["home-score"])
            if self.game_state["away-score"] != final_away_score or self.game_state["home-score"] != final_home_score:
                self.log.warning(
                    f"Calculated scores ({self.game_state['away-score']}, {self.game_state['home-score']}) do not match final scores ({final_away_score}, {final_home_score}) for game {event['game-id']}")

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
        if event["event-type"] == "inning-start":
            self.log.info(f"Inning started: {event['inning']} in game {event['game-id']}")
            # create a new half-inning entry for this game id
            # static: game-id, batting-team, pitching-team, inning
            # initialize the dynamic values: away-score, home-score
            for key in self._def_innings.keys():
                if key in event.keys():
                    self.inning_state[key] = event[key]
                else:
                    if key == "game":
                        self.inning_state[key] = event["game-id"]
                    elif key == "away-score" or key == "home-score":
                        self.inning_state[key] = 0
                    elif key == "inning-running":
                        self.inning_state[key] = True
                    else:
                        self.inning_state[key] = None
            self.game_state["inning"] = event["inning"]

        elif event["event-type"] == "inning-end":
            self.log.info(f"Inning ended: {event['inning']} in game {event['game-id']}")
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
        if event["event-type"] == "pitch":
            self.log.info(f"Pitch: {event['pitch-id']} for event {event['event-id']} of game {event['game-id']}")
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
        if event["event-type"] == "event":
            self.log.info(f"Event: {event['event-id']} in inning {event['inning']} of game {event['game-id']}")
            # create new event entry for game id and event id
            # values: batting-team, pitching-team, inning, event-text, away-score, home-score
            # calculation needed: event-text -> number of outs; runner on bases?

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
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0


def get_sql_engine() -> sqlalchemy.engine.base.Engine:
    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.debug(f"Database connection string: {sql_string}")
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise Exception("No database connection string provided in environment variables.")
    return sql_engine


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _get_insert_statement(table_name: str, data: dict[str, Any], key_columns: list[str]) -> str:
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

    return statement_sql
    # return sqlalchemy.sql.text(statement_sql), bind_params


# --------------------------------------------------------
# Business Logic Wrapper
# --------------------------------------------------------

class BusinessLogicMapper(MapFunction):
    log: logging.Logger

    def map(self, value):
        self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                              log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
        event = json.loads(value)
        self.log.info(f"Received message: {event}")
        game_calc = Calculation(get_sql_engine())
        self.log.info("Initialized Calculation object")
        try:
            result = game_calc.receive_event(event)
            self.log.debug(f"Received total state: {result}")
        except pandas.errors.DatabaseError as e:
            self.log.critical(f"DatabaseError processing event: {e}")
            result = {
                "game-state": None,
                "inning-state": None,
                "pitcher-state": None
            }

        return result


class EventTimestampAssigner(TimestampAssigner):
    log: logging.Logger

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        self.log = set_logger(os.getenv("LOGGER_JOB_NAME", "calculation_job"),
                              log_path=os.getenv("LOGGER_JOB_PATH", "/opt/flink/usrlib/"))
        try:
            this_timestamp = int(event.get("event-timestamp", record_timestamp))
        except (ValueError, TypeError):
            self.log.warning(f"Invalid timestamp in event: {event}")
            this_timestamp = record_timestamp
        self.log.info(f"Extracted timestamp: {this_timestamp} from event: {event}")
        return this_timestamp


def configure_postgres_sink(sql_dml: str, type_info: Types, sql_url: str) -> JdbcSink:
    # TODO: implement Sink as a map function myself, so everything can be logged!
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(sql_url)
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("postgres")
        .with_password("mysecretpassword")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )


# --------------------------------------------------------
# Main
# --------------------------------------------------------

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    LOGGER.info("Setting up Flink Streaming Job for Game Events Calculation")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)
    env.enable_checkpointing(30000)

    LOGGER.info(
        f"Connecting to Kafka at {os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')} and subscribing to topic '{os.getenv('KAFKA_TOPIC', 'game-events')}'")
    kafka_source = KafkaSource.builder()
    LOGGER.debug("Kafka source builder initialized")
    kafka_source = kafka_source.set_bootstrap_servers(
        ":".join([os.getenv('KAFKA_HOST', 'localhost'), os.getenv('KAFKA_PORT', '9092')]))
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

    processed_stream = stream.map(BusinessLogicMapper(), output_type=Types.PICKLED_BYTE_ARRAY())
    # processed_stream.add_sink(postgres_sink)

    game_stream = processed_stream.filter(lambda x: x.get("game-state") is not None)
    inning_stream = processed_stream.filter(lambda x: x.get("inning-state") is not None)
    pitcher_stream = processed_stream.filter(lambda x: x.get("pitcher-state") is not None)

    LOGGER.info("Creating postgres sinks")
    game_def = _load_table_definition("game_state")
    game_sink = configure_postgres_sink(_get_insert_statement("game_state", game_def, ["game"]),
                                        _table_definition_to_types(game_def),
                                        os.getenv("POSTGRES_DB_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres"))
    inning_def = _load_table_definition("innings")
    inning_sink = configure_postgres_sink(_get_insert_statement("innings", inning_def, ["game", "inning"]),
                                          _table_definition_to_types(inning_def),
                                          os.getenv("POSTGRES_DB_JDBC_URL",
                                                    "jdbc:postgresql://postgres:5432/postgres"))
    pitcher_def = _load_table_definition("pitchers")
    pitcher_sink = configure_postgres_sink(_get_insert_statement("pitchers", pitcher_def, ["game", "pitcher"]),
                                           _table_definition_to_types(pitcher_def),
                                           os.getenv("POSTGRES_DB_JDBC_URL",
                                                     "jdbc:postgresql://postgres:5432/postgres"))
    LOGGER.info("Mapping processed stream to sinks")
    game_stream.add_sink(game_sink)
    inning_stream.add_sink(inning_sink)
    pitcher_stream.add_sink(pitcher_sink)

    LOGGER.info('Showing alerts in the console')
    # processed_stream.print()
    #game_stream.print()
    #inning_stream.print()
    #pitcher_stream.print()

    LOGGER.info("Executing Flink Streaming Job for Game Events Calculation")
    env.execute("game-events-calculation")


if __name__ == "__main__":
    main()
