import csv
import logging
import random
import threading
import time

from tweepy import Cursor, TweepError


PAGE_COUNT = 200
TWEET_COUNT = 3000
FRIENDS_COUNT = 50
FOLLOWERS_COUNT = 100
TWEETS_FILE = 'data/tweets.csv'
USERS_FILE = 'data/userids.txt'
LOG_LEVEL = 2

formatter = logging.Formatter("[%(asctime)s | (%(threadName)s)] %(message)s",
                              "%H:%M:%S")
h = logging.StreamHandler()
h.setLevel(LOG_LEVEL)
h.setFormatter(formatter)

logger = logging.getLogger('collector-logger')
logger.setLevel(LOG_LEVEL)
logger.addHandler(h)


class Collector:

    def __init__(self, api):
        self.api = api

    def get_tweets(self, user_id):
        logger.log(LOG_LEVEL, 'Getting tweets for {}'.format(user_id))
        statuses = []

        cursor = Cursor(
            self.api.user_timeline,
            user_id=user_id,
            count=PAGE_COUNT
        )

        for status in cursor.items(TWEET_COUNT):
            statuses.append(self.parse_api_tweet(status))

        return statuses

    def get_users(self, user_id):
        followers = self.get_followers(user_id)
        friends = self.get_friends(user_id)

        if len(followers) > FOLLOWERS_COUNT:
            followers = random.sample(followers, FOLLOWERS_COUNT)
        if len(friends) > FRIENDS_COUNT:
            friends = random.sample(friends, FRIENDS_COUNT)

        return friends + followers

    def get_followers(self, user_id):
        logger.log(LOG_LEVEL, 'Getting followers for {}'.format(user_id))
        followers = []

        cursor = Cursor(
            self.api.followers,
            user_id=user_id
        )

        for user in cursor.items():
            if self.is_potential_target(user):
                followers.append(str(user.id))

        return followers

    def get_friends(self, user_id):
        logger.log(LOG_LEVEL, 'Getting friends for {}'.format(user_id))
        friends = []

        cursor = Cursor(
            self.api.friends,
            user_id=user_id
        )

        for user in cursor.items():
            if self.is_potential_target(user):
                friends.append(str(user.id))

        return friends

    def is_potential_target(self, user):
        return user.followers_count < 3000 \
            and user.statuses_count > 1000 \
            and user.friends_count > 500

    def parse_api_tweet(self, status):
        return {
            'id': str(status.id),
            'user_id': str(status.user.id),
            'screen_name': status.user.screen_name.encode('utf8'),
            'created_at': str(time.mktime(status.created_at.timetuple())),
            'text': status.text.encode('utf8'),
        }

    def save_tweets(self, tweets, filename):
        user = tweets[0]['screen_name']
        logger.log(LOG_LEVEL, 'Saving {}\'s tweets to file'.format(user))

        fieldnames = ['id', 'user_id', 'screen_name', 'created_at', 'text']
        with open(filename, 'ab') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            for t in tweets:
                writer.writerow(t)

    def save_users(self, users, filename):
        with open(filename, 'ab') as f:
            f.write('\n'.join(users))


class TweetCollectorThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None, q=None,
                 collector=None, args=(), kwargs=None, verbose=None):
        super(TweetCollectorThread, self).__init__()
        self.target = target
        self.name = name
        self.q = q
        self.collector = collector
        return

    def run(self):
        while True:
            if not self.q.empty():
                user_id = self.q.get()
                msg = 'Getting {} - {} ids left in queue'.format(
                    user_id, self.q.qsize()
                )
                logger.log(LOG_LEVEL, msg)

                try:
                    tweets = self.collector.get_tweets(user_id=user_id)
                    self.collector.save_tweets(tweets, TWEETS_FILE)
                except TweepError:
                    time.sleep(80)

        return


class UserCollectorThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None, q=None,
                 collector=None, args=(), kwargs=None, verbose=None):
        super(UserCollectorThread, self).__init__()
        self.target = target
        self.name = name
        self.q = q
        self.collector = collector
        return

    def run(self):
        while True:
            if not self.q.empty():
                user_id = self.q.get()
                msg = 'Getting {} - {} ids left in queue'.format(
                    user_id, self.q.qsize()
                )
                logger.log(LOG_LEVEL, msg)

                try:
                    users = self.collector.get_users(user_id=user_id)
                    self.collector.save_users(users, USERS_FILE)

                    for user_id in users:
                        self.q.put(user_id)
                except TweepError:
                    time.sleep(80)

        return
