from Queue import Queue
from settings import CONFIG as CFG
from api import ApiWorkerFactory
from collector import Collector, TweetCollectorThread


def main():
    api_factory = ApiWorkerFactory(
        consumer_key=CFG['CONSUMER_KEY'],
        consumer_secret=CFG['CONSUMER_SECRET']
    )

    tokens = read_tokens()
    q = read_ids_to_queue()

    apis = [api_factory.create(key, secret) for key, secret in tokens]
    collectors = [Collector(api) for api in apis]

    workers = [TweetCollectorThread(name=i, q=q, collector=c)
               for i, c in enumerate(collectors)]

    for worker in workers:
        worker.start()


def read_tokens():
    tokens = []
    with open('tokens.txt', 'r') as f:
        for line in f:
            key, secret = line.strip().split(',')
            tokens.append((key, secret))

    return tokens


def read_ids_to_queue():
    q = Queue()
    with open('data/userids.txt', 'r') as f:
        for line in f:
            user_id = line.strip()
            q.put(user_id)

    return q

if __name__ == '__main__':
    main()
