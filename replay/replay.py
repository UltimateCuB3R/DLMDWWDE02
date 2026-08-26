import threading
import json
from dotenv import load_dotenv
import os
import time
import logging
import sys
from logging.handlers import RotatingFileHandler

import sqlalchemy
import pandas as pd
from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

ENCODING = os.getenv("ENCODING", "utf-8")


class StreamFilter(logging.Filter):
    def filter(self, record):
        if record.levelno in [logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]:
            return True
        else:
            return False


def set_logger(log_name: str, log_level: str = "INFO", log_path: str = "./logs"):
    logger = logging.getLogger(log_name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, log_level.upper()))
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = os.path.join(log_path, f"{log_name}.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 32, backupCount=256, encoding=ENCODING)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('[%(name)s] %(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.addFilter(StreamFilter())
    logger.addHandler(stream_handler)
    return logger


LOGGER = set_logger(os.getenv("LOGGER_NAME", "replay"), os.getenv("LOGGER_LEVEL", "DEBUG"),
                    os.getenv("LOGGER_PATH", "./logs"))


class ReplayControlResponse(BaseModel):
    status: str
    detail: str


class ReplayController:
    def __init__(self):
        self._lock = threading.Lock()
        self._thread = None
        self._pause_event = threading.Event()
        self._stop_requested = False
        self._state = "idle"
        self._error = None
        self._pause_event.set()

    def start(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._stop_requested = False
                self._error = None
                self._pause_event.set()
                self._thread = threading.Thread(target=self._run_worker, daemon=True)
                self._thread.start()
                self._state = "running"
                return self._state, "Replay started."
            if self._state == "paused":
                self._pause_event.set()
                self._state = "running"
                return self._state, "Replay resumed."
            if self._state == "running":
                return self._state, "Replay is already running."
            raise RuntimeError(f"Replay cannot be started from state '{self._state}'.")

    def pause(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                raise ValueError("Replay is not running.")
            if self._state == "paused":
                return self._state, "Replay is already paused."
            self._pause_event.clear()
            self._state = "paused"
            return self._state, "Replay paused."

    def get_status(self):
        with self._lock:
            if self._error:
                return "error", self._error
            return self._state, "ok"

    def _wait_if_paused(self):
        while not self._stop_requested:
            self._pause_event.wait()
            if self._pause_event.is_set():
                return

    def _sleep_interruptible(self, delay_seconds: float):
        end_time = time.time() + max(delay_seconds, 0.0)
        while time.time() < end_time and not self._stop_requested:
            self._wait_if_paused()
            remaining = end_time - time.time()
            time.sleep(min(0.1, max(remaining, 0.0)))

    def _run_worker(self):
        try:
            replay_kafka(self)
        except Exception as exc:
            LOGGER.exception("Replay worker crashed: %s", exc)
            with self._lock:
                self._error = str(exc)
                self._state = "error"
        else:
            with self._lock:
                self._state = "idle"
                self._thread = None


REPLAY_CONTROLLER = ReplayController()


def set_producer(server='localhost', port=9092):
    return KafkaProducer(bootstrap_servers=f'{server}:{port}',
                         key_serializer=lambda k: str(k).encode('utf-8'),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def replay_kafka(controller: ReplayController | None = None):
    replay_delay = float(os.getenv('KAFKA_REPLAY_DELAY', '0.5'))  # delay in seconds between events
    game_replay_offset = float(os.getenv('KAFKA_REPLAY_GAME_OFFSET', '30'))  # delay in seconds between game replays
    replay_offset = float(os.getenv('KAFKA_REPLAY_OFFSET', '30'))

    LOGGER.info(f'Starting replay to Kafka with delay of {replay_delay} seconds between events.')

    producer = set_producer(server=os.getenv('KAFKA_HOST', 'localhost'),
                            port=int(os.getenv('KAFKA_PORT', 9092)))

    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
        LOGGER.info(f'Connected to database with connection string: {sql_string}')
    else:
        LOGGER.critical("No database connection string provided in environment variables.")
        raise Exception("No database connection string provided in environment variables.")
    game_ids = _get_game_ids(sql_engine, limit=int(os.getenv('KAFKA_REPLAY_GAME_LIMIT', '10')))

    if controller:
        controller._sleep_interruptible(replay_offset)
    else:
        time.sleep(replay_offset)  # initial delay before starting the replay

    for game_id in game_ids:
        games, events, pitches = _get_tables_for_game(sql_engine, game_id)
        if controller:
            controller._wait_if_paused()
            _start_replay_thread(games, events, pitches, producer, replay_delay, controller)
            controller._sleep_interruptible(game_replay_offset)
        else:
            # create a thread for each game replay with a delay between game starts derived from env KAFKA_REPLAY_GAME_OFFSET
            replay_thread = threading.Thread(target=_start_replay_thread,
                                             args=(games, events, pitches, producer, replay_delay))
            replay_thread.start()
            time.sleep(game_replay_offset)


def _start_replay_thread(games, events, pitches, producer, replay_delay, controller: ReplayController | None = None):
    for event in replay(games, events, pitches):
        if controller:
            controller._wait_if_paused()
        LOGGER.debug(f'Preparing to send event: {event}')
        future = producer.send(topic=os.getenv('KAFKA_TOPIC', 'game-events'), key=event["game-id"], value=event)
        future.get(timeout=10)  # wait for response, so it's synchronous processing
        LOGGER.info(f'Message sent for event: {event}')
        if controller:
            controller._sleep_interruptible(replay_delay)
        else:
            time.sleep(replay_delay)


def _get_game_ids(sql_in, limit=10):
    games_df = pd.read_sql_query(f'SELECT "Game" FROM public.games LIMIT {limit}', sql_in)
    game_ids = games_df["Game"].tolist()
    LOGGER.info(f'Loaded {len(game_ids)} game ids from database.')
    return game_ids


def _get_tables_for_game(sql_in, game_id: str):
    games_df = pd.read_sql_query(f'SELECT * FROM public.games WHERE "Game" = {game_id} LIMIT 1', sql_in)
    # read the events that fit the game id
    events_df = pd.read_sql_query(f'SELECT * FROM public.events WHERE "Game" = {game_id}', sql_in)
    pitches_df = pd.read_sql_query(f'SELECT * FROM public.pitches WHERE "Game" = {game_id}', sql_in)
    LOGGER.info(
        f'Loaded tables from database: games ({len(games_df)} rows), events ({len(events_df)} rows), pitches ({len(pitches_df)} rows)')

    return games_df, events_df, pitches_df


def _generate_timestamp():
    return int(time.time() * 1000)  # milliseconds since epoch


def replay(table_games: pd.DataFrame, table_events: pd.DataFrame, table_pitches: pd.DataFrame):
    # table_games
    # static: game-id, away, home, stadium, date, location, umpires
    # dynamic: away-record, home-record, away-score, home-score, stats...
    # table_events
    # game-id, pitching-team, batting-team, inning, event-id, events, away, home
    # table_pitches
    # Num(per batter), pitch, type, mph, hitzone, play-bases, play-field, Pitcher, pitching-team, batting-team, inning, event-id, game-id

    # event definition
    # event-type: pitch | event | inning-start | inning-end | game-start | game-end
    # new game -> game-start with static info (game-id!)
    # inning-start -> first event in events with new half-inning
    # pitch -> pitches of events=event-id in num order
    # event -> event from end of pitches for this event (ignore events with empty teams - position changes)
    # inning-end -> after last event in events ending half-inning
    # game-end -> after last event in events for this game

    # iterate through pandas dataframe of table_games and process games data sequentially
    for _, game in table_games.iterrows():
        game_id = game.get("Game")
        game_events = table_events[table_events["Game"] == game_id]
        game_pitches = table_pitches[table_pitches["Game"] == game_id]

        # game start
        current_event = {
            "event-type": "game-start",
            "event-timestamp": _generate_timestamp(),
            "game-id": game_id,
            "away": game.get("away"),
            "home": game.get("home"),
            "stadium": game.get("Stadium"),
            "date": game.get("Date"),
            "location": game.get("Location"),
            "umpires": game.get("Umpires"),
        }
        yield current_event

        # events start
        current_inning = ''
        last_event_of_inning = (
            game_events.dropna(subset=["Inning", "Event Id"])
            .groupby("Inning", sort=False)["Event Id"]
            .max()
            .to_dict()
        )
        for _, event in game_events.iterrows():
            event_id = event.get("Event Id")
            this_inning = event.get("Inning")

            if this_inning != current_inning:
                current_event = {
                    "event-type": "inning-start",
                    "event-timestamp": _generate_timestamp(),
                    "game-id": game_id,
                    "event-id": event_id,
                    "batting-team": event.get("Batting Team"),
                    "pitching-team": event.get("Pitching Team"),
                    "inning": this_inning,
                }
                yield current_event
                current_inning = this_inning

            # all pitches of the event
            event_pitches = game_pitches[game_pitches["Event Id"] == event_id]
            for _, pitch in event_pitches.iterrows():
                try:
                    pitch_speed = int(pitch.get("MPH",""))
                except (ValueError, TypeError):
                    pitch_speed = 0
                current_event = {
                    "event-type": "pitch",
                    "event-timestamp": _generate_timestamp(),
                    "game-id": game_id,
                    "event-id": event_id,
                    "pitcher": pitch.get("Pitcher"),
                    "pitch-id": pitch.get("Num"),
                    "pitch": pitch.get("Pitch"),
                    "pitch-type": pitch.get("Type"),
                    "pitch-speed": pitch_speed,
                    "pitch-location": pitch.get("play-hitzone"),
                    "play-bases": pitch.get("play-bases"),
                    "pitching-team": event.get("Pitching Team")
                }
                yield current_event

            # actual event on last pitch
            current_event = {
                "event-type": "event",
                "event-timestamp": _generate_timestamp(),
                "game-id": game_id,
                "event-id": event_id,
                "batting-team": event.get("Batting Team"),
                "pitching-team": event.get("Pitching Team"),
                "inning": this_inning,
                "event-text": event.get("Events"),
                "away-score": event.get("Away"),
                "home-score": event.get("Home"),
            }
            yield current_event

            # inning end after last event-id of this inning
            if event_id == last_event_of_inning.get(this_inning):
                current_event = {
                    "event-type": "inning-end",
                    "event-timestamp": _generate_timestamp(),
                    "game-id": game_id,
                    "event-id": event_id,
                    "batting-team": event.get("Batting Team"),
                    "pitching-team": event.get("Pitching Team"),
                    "inning": this_inning,
                    "away-score": event.get("Away"),
                    "home-score": event.get("Home"),
                }
                yield current_event

        # game end
        current_event = {
            "event-type": "game-end",
            "event-timestamp": _generate_timestamp(),
            "game-id": game.get("Game"),
            "away-score": game.get("away-score"),
            "home-score": game.get("home-score"),
            "duration": game.get("Duration"),
        }
        yield current_event


app = FastAPI(title="Replay API", version="1.0.0")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/start", response_model=ReplayControlResponse)
def start_replay_endpoint() -> ReplayControlResponse:
    status, detail = REPLAY_CONTROLLER.start()
    if status == "running":
        return ReplayControlResponse(status=status, detail=detail)
    raise HTTPException(status_code=409, detail=detail)


@app.post("/stop", response_model=ReplayControlResponse)
def stop_replay_endpoint() -> ReplayControlResponse:
    try:
        status, detail = REPLAY_CONTROLLER.pause()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ReplayControlResponse(status=status, detail=detail)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
