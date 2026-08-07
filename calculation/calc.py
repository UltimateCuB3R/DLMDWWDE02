from typing import Any
import os

import pandas as pd
import pandas.errors
from kafka import KafkaConsumer
import xml.etree.ElementTree as ET
import json
from dotenv import load_dotenv
import sqlalchemy
from pathlib import Path

load_dotenv()

DB_DEFINITION = "./calculation/table_definition.xml"
ENCODING = os.getenv("ENCODING", "utf-8")


def _load_table_definition(table_name: str) -> dict[str, str]:
    tree = ET.parse(Path(DB_DEFINITION))
    root = tree.getroot()
    table_node = root.find(f"./TABLE[@name='{table_name}']")

    if table_node is None:
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
    else:
        raise Exception("No database connection string provided in environment variables.")

    game_calc = Calculation(sql_engine)
    consumer = set_consumer(server=os.getenv('KAFKA_HOST', 'localhost'),
                            port=int(os.getenv('KAFKA_PORT', 9092)),
                            topic=os.getenv('KAFKA_TOPIC', 'game-events'))
    # consumer.subscribe(['game-events'])
    for msg in consumer:
        print(f'Received message: {msg}')
        try:
            game_calc.receive_event(msg.value)
        except pandas.errors.DatabaseError as e:
            print(f"DatabaseError processing event: {e}")


class Calculation:
    game_state: pd.DataFrame
    innings: pd.DataFrame
    db_con: sqlalchemy.Engine

    def __init__(self, db_con):
        self._def_game_state: dict[str, str] = _load_table_definition("game_state")
        self._def_innings: dict[str, str] = _load_table_definition("innings")
        self.db_con = db_con
        self._init_tables()

    def _init_tables(self):
        print('Initializing Tables...')
        db_inspect = sqlalchemy.inspect(self.db_con)

        self.game_state = pd.DataFrame(columns=list(self._def_game_state.keys()))
        # print(f'[DEBUG] Game state after init: {self.game_state}')
        if not db_inspect.has_table("game_state"):
            sql_result = self.game_state.to_sql('game_state', self.db_con, if_exists='replace', index=False)
            # print(f'SQL result of game_state initialization: {sql_result}')

        self.innings = pd.DataFrame(columns=list(self._def_innings.keys()))
        # print(f'[DEBUG] Innings state after init: {self.innings}')
        if not db_inspect.has_table("innings"):
            sql_result = self.innings.to_sql('innings', self.db_con, if_exists='replace', index=False)
            # print(f'SQL result of innings initialization: {sql_result}')

        print('Tables initialized.')

    def _read_game_state(self, game_id):
        self.game_state = pd.read_sql_query(f'SELECT * FROM public.game_state WHERE "game-id" = {game_id}', self.db_con,
                                            index_col=["game-id"])
        # print(f'[DEBUG] Read Game state: {self.game_state}')

    def _read_game_innings(self, game_id):
        self.innings = pd.read_sql_query(f'SELECT * FROM public.innings WHERE "game-id" = {game_id}', self.db_con,
                                         index_col=["game-id", "inning"])
        # print(f'[DEBUG] Read Innings state: {self.innings}')

    def _save_game_state(self):
        # print(f'[DEBUG] Saving game state: {self.game_state}')
        result = self.game_state.to_sql('game_state', self.db_con, if_exists='replace', index=True)
        # print(f'[DEBUG] result of to_sql (game_state): {result}')

    def _save_game_innings(self):
        # print(f'[DEBUG] Saving innings state: {self.innings}')
        result = self.innings.to_sql('innings', self.db_con, if_exists='replace', index=True)
        # print(f'[DEBUG] result of to_sql (innings): {result}')

    def receive_event(self, event: dict[str, Any]):
        game_id = event["game-id"]
        # read game state and innings via sqlalchemy
        self._read_game_state(game_id)
        self._read_game_innings(game_id)
        # TODO: Pitches

        try:
            print(f'Processing event-type {event["event-type"]}')
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
            print(f"Error processing event: {event}")

    def _process_game(self, event: dict[str, Any]):
        if event["event-type"] == "game-start":
            # check if game is already running
            try:
                self.game_state.loc[event["game-id"]]
            except KeyError:
                pass  # game-id not existing - good!
            else:
                raise ValueError(f"Game with ID {event["game-id"]} already started!")

            # create new game entry with this game id
            # set all static values: game-id, away, home, stadium, date, location, umpires
            # initialize the dynamic values: away-score, home-score, duration
            # set game_running to TRUE
            event_data = []
            for key in self._def_game_state.keys():
                if key in event.keys():
                    event_data.append(event[key])
                else:
                    if key == "game-id":
                        event_data.append(event["game-id"])
                    elif key == "away-score":
                        event_data.append(0)
                    elif key == "home-score":
                        event_data.append(0)
                    elif key == "out":
                        event_data.append(0)
                    elif key == "game-running":
                        event_data.append(True)
                    else:
                        event_data.append(None)

            this_game = pd.Series(index=self._def_game_state.keys(), data=event_data)
            self.game_state.loc[this_game["game-id"]] = this_game

            print(f"Game started: {event["game-id"]}")

        elif event["event-type"] == "game-end":
            print(f"Game ended: {event['game-id']}")
            # set the dynamic values: away-score, home-score, duration
            # set game_running to FALSE
            this_game = self.game_state.loc[event["game-id"]]

            # check if calculated scores match final game scores
            final_away_score = self._to_int(event["away-score"])
            final_home_score = self._to_int(event["home-score"])
            if this_game["away-score"] != final_away_score or this_game["home-score"] != final_home_score:
                print(
                    f"Warning: Calculated scores ({this_game['away-score']}, {this_game['home-score']}) do not match final scores ({final_away_score}, {final_home_score}) for game {event['game-id']}")

            this_game["away-score"] = final_away_score
            this_game["home-score"] = final_home_score
            this_game["duration"] = event["duration"]
            this_game["game-running"] = False
            self.game_state.loc[event["game-id"]] = this_game


        else:
            raise ValueError("Invalid event-type")

    def _process_inning(self, event: dict[str, Any]):
        if event["event-type"] == "inning-start":
            print(f"Inning started: {event['inning']} in game {event['game-id']}")
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
            # print(f'[DEBUG] Inning data: {this_inning}')

            this_game = self.game_state.loc[event["game-id"]]
            this_game.loc["inning"] = event["inning"]
            # print(f'[DEBUG] Game data in this inning: {this_game}')
            self.game_state.loc[event["game-id"]] = this_game

        elif event["event-type"] == "inning-end":
            print(f"Inning ended: {event['inning']} in game {event['game-id']}")
            # set the dynamic values: away-score, home-score
            this_inning = self.innings.loc[(event["game-id"], event["inning"]), :]
            this_inning["away-score"] = self._to_int(event["away-score"])
            this_inning["home-score"] = self._to_int(event["home-score"])
            this_inning["inning-running"] = False
            self.innings.loc[(event["game-id"], event["inning"]), :] = this_inning
            # print(f'[DEBUG] Inning data: {this_inning}')

            this_game = self.game_state.loc[event["game-id"]]
            this_game.loc["out"] = 0
            this_game.loc["count"] = ''
            this_game.loc["runner-1"] = ''
            this_game.loc["runner-2"] = ''
            this_game.loc["runner-3"] = ''
            # print(f'[DEBUG] Game data in this inning: {this_game}')

            self.game_state.loc[event["game-id"]] = this_game
        else:
            raise ValueError("Invalid event-type")

    def _process_pitch(self, event: dict[str, Any]):
        if event["event-type"] == "pitch":
            print(f"Pitch: {event['pitch-id']} for event {event['event-id']} of game {event['game-id']}")
            # create new pitch entry for game id and event id
            # values: pitcher, pitch-id, pitch, pitch-type, pitch-speed, pitch-location
            # calculation needed: pitch -> call (strike/ball)

            # TODO: Implementation needed
        else:
            raise ValueError("Invalid event-type")

    def _process_event(self, event: dict[str, Any]):
        if event["event-type"] == "event":
            print(f"Event: {event['event-id']} in inning {event['inning']} of game {event['game-id']}")
            # create new event entry for game id and event id
            # values: batting-team, pitching-team, inning, event-text, away-score, home-score
            # calculation needed: event-text -> number of outs; runner on bases?
            this_game = self.game_state.loc[event["game-id"]]
            this_game.loc["away-score"] = self._to_int(event["away-score"])
            this_game.loc["home-score"] = self._to_int(event["home-score"])
            event_text = "" if pd.isna(event["event-text"]) else str(event["event-text"]).lower()
            if "double play" in event_text:
                this_game.loc["out"] += 2
            elif "out" in event_text:
                this_game.loc["out"] += 1
            if "pitching" in event_text:
                this_game.loc["pitcher"] = event["event-text"].split("pitching")[0].strip()
            print(f'current outs: {this_game.loc["out"]}')
            self.game_state.loc[event["game-id"]] = this_game
        else:
            raise ValueError("Invalid event-type")

    @staticmethod
    def _to_int(value: Any) -> int:
        if pd.isna(value):
            return 0
        numeric_value = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric_value):
            return 0
        return int(numeric_value)
