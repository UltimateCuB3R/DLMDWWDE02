from ingestion import ingest
from replay import replay
from calculation import calc
from threading import Thread
import time
from dotenv import load_dotenv

load_dotenv()

LOAD_CSV = False
REPLAY = True
CONSUME = False

if __name__ == '__main__':
    print('Data Engineering')
    if LOAD_CSV:
        print('Starting Load CSV...')
        tables = ingest.start_ingestion('raw_data/')
        print(f'{len(tables)} tables loaded.')
    else:
        print('No Load CSV started.')

    if CONSUME:
        print('Starting consumer thread...')
        consumer_thread = Thread(target=calc.start_calculation, daemon=True)
        consumer_thread.start()
        time.sleep(2)  # wait 2s for Consumer
        print('Consumer was started.')
    else:
        print('No Consumer started.')

    if REPLAY:
        print('Starting Replay to Kafka...')
        replay.replay_kafka()
        print('Replay ended.')
    else:
        print('No Replay started.')
