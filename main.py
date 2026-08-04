from ingestion import ingest
from replay import replay
from data import database
from calculation import calc
import pandas as pd

LOAD_CSV = False
WRITE_SQL = False
REPLAY = True

if __name__ == '__main__':
    print('Data Engineering')

    # games_table = ingest.load_csv_file("data/games.csv", "games")
    # print(games_table)

    if LOAD_CSV:
        games_table = ingest.load_csv_into_table("raw_data/games.csv", "games")
        print('Games Table loaded from csv')
        events_table = ingest.load_csv_into_table("raw_data/events.csv", "events")
        print('Events Table loaded from csv')
        pitches_table = ingest.load_csv_into_table("raw_data/pitches.csv", "pitches")
        print('Pitches Table loaded from csv')
        """# Check data types of each column in the DataFrame
        data_types = pitches_table.df.dtypes
        print("Data Types (pitches):")
        print(data_types)
        data_types = events_table.df.dtypes
        print("Data Types (events):")
        print(data_types)
        data_types = games_table.df.dtypes
        print("Data Types (games):")
        print(data_types)"""

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
        sql_in = database.create_connection('data/ingest.db')
        print('SqlIn - Connection to db')
        games_df = pd.read_sql_query('SELECT * FROM games LIMIT 1', sql_in)
        # read the events that fit the game ids
        #events_df = pd.read_sql_query('SELECT * FROM events WHERE Game = 360403123', sql_in)
        game_ids = games_df["Game"].tolist()
        events_df = pd.read_sql_query(f'SELECT * FROM events WHERE Game IN ({",".join(map(str, game_ids))})', sql_in)
        pitches_df = pd.read_sql_query(f'SELECT * FROM pitches WHERE Game IN ({",".join(map(str, game_ids))})', sql_in)
        print('SqlIn - All Tables loaded from db')

        games_table = ingest.PdTable(table_name='games', df=games_df)
        events_table = ingest.PdTable(table_name='events', df=events_df)
        pitches_table = ingest.PdTable(table_name='pitches', df=pitches_df)

    if REPLAY:
        game_calc = calc.Calculation(None)
        for event in replay.replay([games_table, events_table, pitches_table]):
            # print(event)
            game_calc.receive_event(event)
    else:
        print('No Replay started.')

