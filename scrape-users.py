from Queue import Queue
from settings import CONFIG as CFG
from api import ApiWorkerFactory
from collector import Collector, UserCollectorThread


def main():
    api_factory = ApiWorkerFactory(
        consumer_key=CFG['CONSUMER_KEY'],
        consumer_secret=CFG['CONSUMER_SECRET']
    )

    tokens = read_tokens()
    q = read_seeds_to_queue()

    apis = [api_factory.create(key, secret) for key, secret in tokens]
    collectors = [Collector(api) for api in apis]

    workers = [UserCollectorThread(name=i, q=q, collector=c)
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


def read_seeds_to_queue():
    q = Queue()
    q.put('537439729')
    q.put('181269400')
    q.put('327487581')
    q.put('1417290926')
    q.put('504813703')
    return q

if __name__ == '__main__':
    main()
