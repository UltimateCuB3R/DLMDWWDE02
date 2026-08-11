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

load_dotenv()

DB_DEFINITION = "./calculation/table_definition.xml"
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


LOGGER = set_logger(os.getenv("LOGGER_NAME", "calculation"), os.getenv("LOGGER_LEVEL", "INFO"),
                    os.getenv("LOGGER_PATH", "./logs"))


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
            game_state = game_calc.receive_event(msg.value)
            LOGGER.debug(f"Received game state: {game_state}")
        except pandas.errors.DatabaseError as e:
            LOGGER.critical(f"DatabaseError processing event: {e}")


class Calculation:
    game_state: dict
    innings: pd.DataFrame
    db_con: sqlalchemy.Engine

    def __init__(self, db_con):
        self._def_game_state: dict[str, str] = _load_table_definition("game_state")
        self._def_innings: dict[str, str] = _load_table_definition("innings")
        self.db_con = db_con
        self._init_tables()

    def _init_tables(self):
        LOGGER.info('Initializing Tables...')
        db_inspect = sqlalchemy.inspect(self.db_con)

        self.game_state = {key: None for key in self._def_game_state.keys()}
        # LOGGER.debug(f'Game state after init: {self.game_state}')
        if not db_inspect.has_table("game_state"):
            sql_result = pd.DataFrame(columns=list(self._def_game_state.keys())).to_sql('game_state', self.db_con,
                                                                                        if_exists='replace',
                                                                                        index=False)
            # LOGGER.debug(f'SQL result of game_state initialization: {sql_result}')

        self.innings = pd.DataFrame(columns=list(self._def_innings.keys()))
        # LOGGER.debug(f'[DEBUG] Innings state after init: {self.innings}')
        if not db_inspect.has_table("innings"):
            sql_result = self.innings.to_sql('innings', self.db_con, if_exists='replace', index=False)
            # LOGGER.debug(f'SQL result of innings initialization: {sql_result}')

        LOGGER.info('Tables initialized.')

    def _read_game_state(self, game_id):
        game_state = pd.read_sql_query(f'SELECT * FROM public.game_state WHERE "game-id" = {game_id}', self.db_con,
                                       index_col=["game-id"])
        # determine length of dataframe, if 1 then read first row as dict, if 0 then create new row with default values, if >1 then raise error
        if len(game_state) == 1:
            self.game_state = game_state.iloc[0].to_dict()
        elif len(game_state) == 0:
            self.game_state = {key: None for key in self._def_game_state.keys()}
        else:
            LOGGER.critical(f"Unexpected number of rows in game_state for game_id {game_id}: {len(game_state)}")
            raise ValueError("Unexpected number of rows in game_state")
        # LOGGER.debug(f'Read Game state: {self.game_state}')

    def _read_game_innings(self, game_id):
        self.innings = pd.read_sql_query(f'SELECT * FROM public.innings WHERE "game-id" = {game_id}', self.db_con,
                                         index_col=["game-id", "inning"])
        # LOGGER.debug(f'Read Innings state: {self.innings}')

    def _save_game_state(self):
        # LOGGER.debug(f'Saving game state: {self.game_state}')
        result = pd.DataFrame([self.game_state]).to_sql('game_state', self.db_con, if_exists='replace', index=False)
        # LOGGER.debug(f'result of to_sql (game_state): {result}')

    def _save_game_innings(self):
        # LOGGER.debug(f'Saving innings state: {self.innings}')
        result = self.innings.to_sql('innings', self.db_con, if_exists='replace', index=True)
        # LOGGER.debug(f'result of to_sql (innings): {result}')

    def receive_event(self, event: dict[str, Any]):
        game_id = event["game-id"]
        # read game state and innings via sqlalchemy
        self._read_game_state(game_id)
        self._read_game_innings(game_id)
        # TODO: Pitch counter per Pitcher per Game

        try:
            LOGGER.info(f'Processing event-type {event["event-type"]}')
            match event["event-type"]:
                case "game-start" | "game-end":
                    self._process_game(event)
                    self._save_game_state()
                case "inning-start" | "inning-end":
                    self._process_inning(event)
                    self._save_game_state()
                    self._save_game_innings()
                case "pitch":
                    self._process_pitch(event)
                case "event":
                    self._process_event(event)
                    self._save_game_state()
        except ValueError:
            # TODO: return information about broken order or event-type
            LOGGER.critical(f"Error processing event: {event}")

        return self.game_state

    def _process_game(self, event: dict[str, Any]):
        if event["event-type"] == "game-start":
            LOGGER.info(f"Game started: {event['game-id']}")
            # check if game is already running
            if self.game_state["game-id"] == event["game-id"]:
                LOGGER.critical(f"Game with ID {event['game-id']} already started!")
                raise ValueError(f"Game with ID {event['game-id']} already started!")

            # create new game entry with this game id
            # set all static values: game-id, away, home, stadium, date, location, umpires
            # initialize the dynamic values: away-score, home-score, duration
            # set game_running to TRUE
            for key in self._def_game_state.keys():
                if key in event.keys():
                    self.game_state[key] = event[key]
                else:
                    if key == "game-id":
                        self.game_state[key] = event["game-id"]
                    elif key == "away-score" or key == "home-score" or key == "out":
                        self.game_state[key] = 0
                    elif key == "game-running":
                        self.game_state[key] = True
                    else:
                        self.game_state[key] = None



        elif event["event-type"] == "game-end":
            LOGGER.info(f"Game ended: {event['game-id']}")
            # check if calculated scores match final game scores
            final_away_score = self._to_int(event["away-score"])
            final_home_score = self._to_int(event["home-score"])
            if self.game_state["away-score"] != final_away_score or self.game_state["home-score"] != final_home_score:
                LOGGER.warning(
                    f"Calculated scores ({self.game_state['away-score']}, {self.game_state['home-score']}) do not match final scores ({final_away_score}, {final_home_score}) for game {event['game-id']}")

            # set the dynamic values: away-score, home-score, duration
            # set game_running to FALSE
            self.game_state["away-score"] = final_away_score
            self.game_state["home-score"] = final_home_score
            self.game_state["duration"] = event["duration"]
            self.game_state["game-running"] = False


        else:
            LOGGER.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_inning(self, event: dict[str, Any]):
        if event["event-type"] == "inning-start":
            LOGGER.info(f"Inning started: {event['inning']} in game {event['game-id']}")
            # create a new half-inning entry for this game id
            # static: game-id, batting-team, pitching-team, inning
            # initialize the dynamic values: away-score, home-score
            event_data = []
            for key in self._def_innings.keys():
                if key in event.keys():
                    event_data.append(event[key])
                else:
                    if key == "game-id":
                        event_data.append(event["game-id"])
                    elif key == "away-score":
                        event_data.append(0)
                    elif key == "home-score":
                        event_data.append(0)
                    elif key == "inning-running":
                        event_data.append(True)
                    else:
                        event_data.append(None)
            this_inning = pd.Series(index=self._def_innings.keys(), data=event_data)
            self.innings.loc[(event["game-id"], event["inning"]), :] = this_inning
            # LOGGER.debug(f'Inning data: {this_inning}')

            self.game_state["inning"] = event["inning"]

        elif event["event-type"] == "inning-end":
            LOGGER.info(f"Inning ended: {event['inning']} in game {event['game-id']}")
            # set the dynamic values: away-score, home-score
            this_inning = self.innings.loc[(event["game-id"], event["inning"]), :]
            this_inning["away-score"] = self._to_int(event["away-score"])
            this_inning["home-score"] = self._to_int(event["home-score"])
            this_inning["inning-running"] = False
            self.innings.loc[(event["game-id"], event["inning"]), :] = this_inning
            # LOGGER.debug(f'Inning data: {this_inning}')

            self.game_state["out"] = 0
            self.game_state["count"] = ''
            self.game_state["runner-1"] = ''
            self.game_state["runner-2"] = ''
            self.game_state["runner-3"] = ''
            # LOGGER.debug(f'Game data in this inning: {self.game_state}')
        else:
            LOGGER.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_pitch(self, event: dict[str, Any]):
        if event["event-type"] == "pitch":
            LOGGER.info(f"Pitch: {event['pitch-id']} for event {event['event-id']} of game {event['game-id']}")
            # create new pitch entry for game id and event id
            # values: pitcher, pitch-id, pitch, pitch-type, pitch-speed, pitch-location
            # calculation needed: pitch -> call (strike/ball)
            pitch = str(event["pitch"]).lower()
            if "strike" in pitch or "bunted foul" in pitch:
                self.game_state["strikes"] += 1
            elif "ball" in pitch and not "foul" in pitch and not "hit by" in pitch:
                self.game_state["balls"] += 1
            elif "foul" in pitch and not "foul out" in pitch:
                if self.game_state["strikes"] < 2:
                    self.game_state["strikes"] += 1
            self.game_state["count"] = f"{self.game_state['balls']}-{self.game_state['strikes']}"
            self.game_state["pitcher"] = event["pitcher"]
            # pitch_count["pitcher"] += 1  # TODO: implement pitch count per pitcher
            self.game_state["pitch-type"] = event["pitch-type"]
            self.game_state["pitch-speed"] = event["pitch-speed"]
            if event["pitch-location"] is not None:
                match = re.search(
                    r"top:\s*([0-9]+(?:\.[0-9]+)?)px;\s*right:\s*([0-9]+(?:\.[0-9]+)?)px;",
                    str(event["pitch-location"]),
                )
                if match:
                    self.game_state["pitch_y"] = float(match.group(1))
                    self.game_state["pitch_x"] = float(match.group(2))
            if event["play-bases"] is not None:
                self.game_state["runner-1"] = "X" if "1" in str(event["play-bases"]) else ""
                self.game_state["runner-2"] = "X" if "2" in str(event["play-bases"]) else ""
                self.game_state["runner-3"] = "X" if "3" in str(event["play-bases"]) else ""
        else:
            LOGGER.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    def _process_event(self, event: dict[str, Any]):
        if event["event-type"] == "event":
            LOGGER.info(f"Event: {event['event-id']} in inning {event['inning']} of game {event['game-id']}")
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
            LOGGER.info(f'current outs: {self.game_state["out"]}')
        else:
            LOGGER.critical(f"Invalid event-type: {event['event-type']}")
            raise ValueError("Invalid event-type")

    @staticmethod
    def _to_int(value: Any) -> int:
        if pd.isna(value):
            return 0
        numeric_value = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric_value):
            return 0
        return int(numeric_value)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def start_pyflink():


    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Define your Flink job here
    # For example, you can read from Kafka, process the data, and write to a sink

    # Execute the Flink job
    env.execute("Flink Job")