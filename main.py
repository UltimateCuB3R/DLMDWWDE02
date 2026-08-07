from ingestion import ingest
from replay import replay
from data import database
from calculation import calc
import pandas as pd
from threading import Thread
import time

LOAD_CSV = False
SQL_TYPE = 'PGSQL'  # deprecated
WRITE_SQL = False  # deprecated
REPLAY = True
KAFKA = True  # deprecated
CONSUME = True

if __name__ == '__main__':
    print('Data Engineering')
    if LOAD_CSV:
        tables = ingest.start_ingestion('raw_data/')
        print(f'{len(tables)} tables loaded.')
    else:
        print('No Load CSV started.')

    if CONSUME:
        print('Starting consumer thread...')
        consumer_thread = Thread(target=calc.start_calculation, daemon=True)
        consumer_thread.start()
        time.sleep(2) # wait 2s for Consumer
        print('Consumer was started.')

    if REPLAY:
        print('Starting Replay to Kafka...')
        replay.replay_kafka()
        print('Replay ended.')

    else:
        print('No Replay started.')

if __name__ == 'old':
    if LOAD_CSV:
        games_table = ingest._load_csv_into_table("raw_data/games.csv", "games")
        print('Games Table loaded from csv')
        events_table = ingest._load_csv_into_table("raw_data/events.csv", "events")
        print('Events Table loaded from csv')
        pitches_table = ingest._load_csv_into_table("raw_data/pitches.csv", "pitches")
        print('Pitches Table loaded from csv')

        if SQL_TYPE == 'PGSQL':
            # sql_in = database.create_postgres_connection(dbname="postgres", user="postgres", password="mysecretpassword")
            sql_in = database.create_engine("postgresql+psycopg://postgres:mysecretpassword@localhost:5432/postgres")
        else:
            sql_in = database.create_connection('data/ingest.db')
        print('SqlIn')
        if WRITE_SQL:
            games_table.df.to_sql('games', sql_in, if_exists='replace', index=False)
            print('SqlIn - Games written to SQL')
            events_table.df.to_sql('events', sql_in, if_exists='replace', index=False)
            print('SqlIn - Events written to SQL')
            pitches_table.df.to_sql('pitches', sql_in, if_exists='replace', index=False)
            print('SqlIn - Pitches written to SQL')
    else:
        if SQL_TYPE == 'PGSQL':
            sql_in = database.create_engine("postgresql+psycopg://postgres:mysecretpassword@localhost:5432/postgres")
        else:
            sql_in = database.create_connection('data/ingest.db')
        print('SqlIn - Connection to db')
        games_df = pd.read_sql_query('SELECT * FROM public.games LIMIT 5', sql_in)
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

    if REPLAY:
        if KAFKA:
            replay.replay_kafka(sql_in)
            print('Replay ended.')
        else:
            game_calc = calc.Calculation(None)
            for event in replay.replay([games_table, events_table, pitches_table]):
                # print(event)
                game_calc.receive_event(event)
    else:
        print('No Replay started.')

    if CONSUME:
        print('Consume started.')
        calc.consume_kafka(None)
        print('Consume ended.')
