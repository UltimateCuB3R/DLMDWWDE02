from typing import Any

import pandas as pd
from ingestion.ingest import load_dtypes_for_table
from kafka import KafkaConsumer

OUTPUT_GAMESTATE = False

def set_consumer(server='localhost', port=9092, topic='game-events'):
    return KafkaConsumer(topic,
                         bootstrap_servers=f'{server}:{port}',
                         key_deserializer=lambda k: str(k).encode('utf-8'),
                         value_deserializer=lambda v: v.decode('utf-8'))

def consume_kafka(sql_out):
    game_calc = Calculation(sql_out)
    consumer = set_consumer(server='localhost', port=9092, topic='game-events')
    consumer.subscribe(['game-events'])
    for msg in consumer:
        print(f'Received message: {msg}')
        game_calc.receive_event(msg.value)

class Calculation:
    game_state: pd.DataFrame

    def __init__(self, db_con):
        self._def_game_state = load_dtypes_for_table("game_state")
        self._def_innings = load_dtypes_for_table("innings")
        self._def_game_state["away-score"] = "Int64"
        self._def_game_state["home-score"] = "Int64"
        self._def_game_state["out"] = "Int64"
        self._def_innings["away-score"] = "Int64"
        self._def_innings["home-score"] = "Int64"
        self.db_con = db_con  # TODO: replace with correct db logic
        self.game_state = pd.DataFrame(columns=self._def_game_state.keys()).astype(self._def_game_state)
        self.innings = pd.DataFrame(columns=self._def_innings.keys()).astype(self._def_innings)
        self.innings.set_index(["game-id", "inning"], inplace=True, drop=False)

    def receive_event(self, event: dict[str, Any]):
        match event["event-type"]:
            case "game-start":
                self._process_game(event)
                if OUTPUT_GAMESTATE: print(self.game_state)
            case "game-end":
                self._process_game(event)
                if OUTPUT_GAMESTATE: print(self.game_state)
            case "inning-start":
                self._process_inning(event)
                if OUTPUT_GAMESTATE: print(self.innings)
            case "inning-end":
                self._process_inning(event)
                if OUTPUT_GAMESTATE: print(self.innings)
            case "pitch":
                self._process_pitch(event)
            case "event":
                self._process_event(event)

        # TODO: finally save df data to database
        # self.db_con ...

    def _process_game(self, event: dict[str, Any]):
        if event["event-type"] == "game-start":
            # TODO: check if game is already running!

            # create new game entry with this game id
            # set all static values: game-id, away, home, stadium, date, location, umpires
            # initialize the dynamic values: away-score, home-score, duration
            # set game_running to TRUE
            event_data = []
            for key in self._def_game_state.keys():
                if key in event.keys():
                    event_data.append(event[key])
                else:
                    if key == "away-score":
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
            self.game_state.loc[event["game-id"]] = this_game
            print(f"Game started: {event['game-id']}")

        elif event["event-type"] == "game-end":
            print(f"Game ended: {event['game-id']}")
            # set the dynamic values: away-score, home-score, duration
            # set game_running to FALSE
            this_game = self.game_state.loc[event["game-id"]]
            # TODO: check if calculated scores match final game scores
            this_game["away-score"] = self._to_int(event["away-score"])
            this_game["home-score"] = self._to_int(event["home-score"])
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
                    if key == "away-score":
                        event_data.append(0)
                    elif key == "home-score":
                        event_data.append(0)
                    elif key == "inning-running":
                        event_data.append(True)
                    else:
                        event_data.append(None)
            this_inning = pd.Series(index=self._def_innings.keys(), data=event_data)
            self.innings.loc[(event["game-id"], event["inning"]), :] = this_inning

            this_game = self.game_state.loc[event["game-id"]]
            this_game["inning"] = event["inning"]
            self.game_state.loc[event["game-id"]] = this_game

        elif event["event-type"] == "inning-end":
            print(f"Inning ended: {event['inning']} in game {event['game-id']}")
            # set the dynamic values: away-score, home-score
            this_inning = self.innings.loc[(event["game-id"], event["inning"]), :]
            this_inning["away-score"] = self._to_int(event["away-score"])
            this_inning["home-score"] = self._to_int(event["home-score"])
            this_inning["inning-running"] = False
            self.innings.loc[(event["game-id"], event["inning"]), :] = this_inning

            this_game = self.game_state.loc[event["game-id"]]
            this_game["out"] = 0
            this_game["count"] = ''
            this_game["runner-1"] = ''
            this_game["runner-2"] = ''
            this_game["runner-3"] = ''

            self.game_state.loc[event["game-id"]] = this_game
        else:
            raise ValueError("Invalid event-type")

    def _process_pitch(self, event: dict[str, Any]):
        if event["event-type"] == "pitch":
            print(f"Pitch: {event['pitch-id']} for event {event['event-id']} of game {event['game-id']}")
            # create new pitch entry for game id and event id
            # values: pitcher, pitch-id, pitch, pitch-type, pitch-speed, pitch-location
            # calculation needed: pitch -> call (strike/ball)
        else:
            raise ValueError("Invalid event-type")

    def _process_event(self, event: dict[str, Any]):
        if event["event-type"] == "event":
            print(f"Event: {event['event-id']} in inning {event['inning']} of game {event['game-id']}")
            # create new event entry for game id and event id
            # values: batting-team, pitching-team, inning, event-text, away-score, home-score
            # calculation needed: event-text -> number of outs; runner on bases?
            this_game = self.game_state.loc[event["game-id"]]
            this_game["away-score"] += self._to_int(event["away-score"])
            this_game["home-score"] += self._to_int(event["home-score"])
            event_text = "" if pd.isna(event["event-text"]) else str(event["event-text"]).lower()
            if "double play" in event_text:
                this_game["out"] += 2
            elif "out" in event_text:
                this_game["out"] += 1
            if "pitching" in event_text:
                this_game["pitcher"] = event["event-text"].split("pitching")[0].strip()
            print(f'current outs: {this_game["out"]}')
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
