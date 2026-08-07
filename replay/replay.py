import sqlalchemy

from ingestion import ingest
import pandas as pd
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os
import time

load_dotenv()


def set_producer(server='localhost', port=9092):
    return KafkaProducer(bootstrap_servers=f'{server}:{port}',
                         key_serializer=lambda k: str(k).encode('utf-8'),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def replay_kafka():
    replay_delay = float(os.getenv('KAFKA_REPLAY_DELAY', '0.5'))  # delay in seconds between events

    producer = set_producer(server=os.getenv('KAFKA_HOST', 'localhost'),
                            port=int(os.getenv('KAFKA_PORT', 9092)))

    sql_string = os.getenv("POSTGRES_DB_CONNECT_STRING")
    if sql_string:
        sql_engine = sqlalchemy.create_engine(sql_string)
    else:
        raise Exception("No database connection string provided in environment variables.")
    tables = _get_tables(sql_engine)

    # kafka_key = 0  # internal replay key to secure right order
    for event in replay(tables):
        future = producer.send(topic=os.getenv('KAFKA_TOPIC', 'game-events'), key=event["game-id"], value=event)
        result = future.get(timeout=10)  # wait for response, so it's synchronous processing
        print(f'Message sent for event: {event}')
        # kafka_key += 1
        time.sleep(replay_delay)


def _get_tables(sql_in) -> list[ingest.PdTable]:
    tables = []
    games_df = pd.read_sql_query('SELECT * FROM public.games LIMIT 2', sql_in)  # TODO: remove LIMIT 2 in production
    # read the events that fit the game ids
    game_ids = games_df["Game"].tolist()
    events_df = pd.read_sql_query(f'SELECT * FROM public.events WHERE "Game" IN ({",".join(map(str, game_ids))})',
                                  sql_in)
    pitches_df = pd.read_sql_query(
        f'SELECT * FROM public.pitches WHERE "Game" IN ({",".join(map(str, game_ids))})', sql_in)
    print('SqlIn - All Tables loaded from db')

    games_table = ingest.PdTable(table_name='games', df=games_df)
    events_table = ingest.PdTable(table_name='events', df=events_df)
    pitches_table = ingest.PdTable(table_name='pitches', df=pitches_df)
    tables.extend([games_table, events_table, pitches_table])
    return tables


def replay(tables: list[ingest.PdTable]):
    # replay mode is always pitch by pitch
    table_games: pd.DataFrame | None = None
    table_events: pd.DataFrame | None = None
    table_pitches: pd.DataFrame | None = None

    for table in tables:
        match table.table_name:
            case "games":
                table_games = table.df
                # static: game-id, away, home, stadium, date, location, umpires
                # dynamic: away-record, home-record, away-score, home-score, stats...
            case "events":
                table_events = table.df
                # game-id, pitching-team, batting-team, inning, event-id, events, away, home
            case "pitches":
                table_pitches = table.df
                # Num(per batter), pitch, type, mph, hitzone, play-bases, play-field, Pitcher, pitching-team, batting-team, inning, event-id, game-id

    # event definition
    # event-type: pitch | event | inning-start | inning-end | game-start | game-end
    # easy mode: anonymous batters
    # new game -> game-start with static info (game-id!)
    # inning-start -> first event in events with new half-inning
    # pitch -> pitches of events=event-id in num order
    # event -> event from end of pitches for this event (ignore events with empty teams - position changes)
    # inning-end -> after last event in events ending half-inning

    # iterate through pandas dataframe of table_games and process games data sequentially
    if table_games is None:
        raise ValueError("Missing required table: games")
    elif table_events is None:
        raise ValueError("Missing required table: events")
    elif table_pitches is None:
        raise ValueError("Missing required table: pitches")

    for _, game in table_games.iterrows():
        game_id = game.get("Game")
        game_events = table_events[table_events["Game"] == game_id]
        game_pitches = table_pitches[table_pitches["Game"] == game_id]

        # game start
        current_event = {
            "event-type": "game-start",
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
                    "game-id": game_id,
                    "event-id": event_id,
                    "pitcher": pitch.get("Pitcher"),
                    "pitch-id": pitch.get("Num"),
                    "pitch": pitch.get("Pitch"),
                    "pitch-type": pitch.get("Type"),
                    "pitch-speed": pitch.get("MPH"),
                    "pitch-location": pitch.get("play-hitzone"),
                }
                yield current_event

            # actual event on last pitch
            current_event = {
                "event-type": "event",
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
            "game-id": game.get("Game"),
            "away-score": game.get("away-score"),
            "home-score": game.get("home-score"),
            "duration": game.get("Duration"),
        }
        yield current_event
