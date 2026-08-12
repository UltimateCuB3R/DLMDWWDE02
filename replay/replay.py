import sqlalchemy
import pandas as pd
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os
import time
import logging
import sys
from logging.handlers import RotatingFileHandler

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


def set_producer(server='localhost', port=9092):
    return KafkaProducer(bootstrap_servers=f'{server}:{port}',
                         key_serializer=lambda k: str(k).encode('utf-8'),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def replay_kafka():
    replay_delay = float(os.getenv('KAFKA_REPLAY_DELAY', '0.5'))  # delay in seconds between events
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
    games, events, pitches = _get_tables(sql_engine)

    # TODO: Start multiple replays after each other, but with different game-ids, so that we can simulate multiple games at the same time
    for event in replay(games, events, pitches):
        future = producer.send(topic=os.getenv('KAFKA_TOPIC', 'game-events'), key=event["game-id"], value=event)
        result = future.get(timeout=10)  # wait for response, so it's synchronous processing
        LOGGER.info(f'Message sent for event: {event}')
        time.sleep(replay_delay)


def _get_tables(sql_in):
    games_df = pd.read_sql_query('SELECT * FROM public.games LIMIT 2', sql_in)  # TODO: remove LIMIT 2 in production
    # read the events that fit the game ids
    game_ids = games_df["Game"].tolist()
    events_df = pd.read_sql_query(f'SELECT * FROM public.events WHERE "Game" IN ({",".join(map(str, game_ids))})',
                                  sql_in)
    pitches_df = pd.read_sql_query(
        f'SELECT * FROM public.pitches WHERE "Game" IN ({",".join(map(str, game_ids))})', sql_in)
    LOGGER.info(f'Loaded tables from database: games ({len(games_df)} rows), events ({len(events_df)} rows), pitches ({len(pitches_df)} rows)')

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
                    # "event-text": event.get("events")
                }
                yield current_event
                current_inning = this_inning

            # all pitches of the event
            event_pitches = game_pitches[game_pitches["Event Id"] == event_id]
            for _, pitch in event_pitches.iterrows():
                current_event = {
                    "event-type": "pitch",
                    "event-timestamp": _generate_timestamp(),
                    "game-id": game_id,
                    "event-id": event_id,
                    "pitcher": pitch.get("Pitcher"),
                    "pitch-id": pitch.get("Num"),
                    "pitch": pitch.get("Pitch"),
                    "pitch-type": pitch.get("Type"),
                    "pitch-speed": pitch.get("MPH"),
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
