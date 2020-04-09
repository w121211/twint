import datetime
import sys
import datetime
from dataclasses import dataclass, astuple, field
from typing import Union, Optional

import psycopg2
from omegaconf import DictConfig
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


CREATE_TABLE_RSS = """
    CREATE TABLE IF NOT EXISTS
        rss(
            id serial PRIMARY KEY,
            url VARCHAR (255) UNIQUE NOT NULL,
            ticker VARCHAR (255),
            freq INTEGER,
            last_published_at TIMESTAMP,
            fetched_at TIMESTAMP
        );
"""

CREATE_TABLE_ARTICLES = """
    CREATE TABLE IF NOT EXISTS
        articles(
            id serial PRIMARY KEY,
            url VARCHAR (255) UNIQUE NOT NULL,
            rss_id VARCHAR(255),
            rss_title TEXT,
            rss_summary TEXT,
            rss_published_at TIMESTAMP,
            rss_tickers TEXT,
            np_title TEXT,
            np_text TEXT,
            np_published_at TIMESTAMP,
            raw TEXT,
            fetched_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
"""


@dataclass
class Rss:
    url: str
    ticker: str = None
    freq: int = None
    last_published_at: datetime.datetime = None
    fetched_at: datetime.datetime = None
    _id: int = None
    # fetched_at: datetime.datetime = field(
    #     default_factory=datetime.datetime.now, init=False)


@dataclass
class Article:
    url: str
    rss_id: str = None
    rss_title: str = None
    rss_summary: str = None
    rss_published_at: datetime.datetime = None
    rss_tickers: str = None
    np_title: str = None
    np_text: str = None
    np_published_at: datetime.datetime = None
    raw: str = None
    fetched_at: datetime.datetime = None
    updated_at: datetime.datetime = None


class Client:
    def __init__(self, cfg: DictConfig) -> None:
        self.conn = psycopg2.connect(
            user=cfg.user, password=cfg.password, host=cfg.host, dbname=cfg.dbname)

    def close(self):
        self.conn.close()

    @classmethod
    def create_db(cls, cfg: DictConfig):
        print("Createing DB")
        with psycopg2.connect(user=cfg.user, password=cfg.password, host=cfg.host) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {};").format(
                    sql.Identifier(cfg.dbname)))

        print("Creating tables")
        with psycopg2.connect(user=cfg.user, password=cfg.password, host=cfg.host, dbname=cfg.dbname) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_RSS)
                cur.execute(CREATE_TABLE_ARTICLES)

    @classmethod
    def drop_db(cls, cfg: DictConfig):
        print("Droping DB")
        with psycopg2.connect(user=cfg.user, password=cfg.password, host=cfg.host) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {};").format(
                    sql.Identifier(cfg.dbname)))

    def get_rss(self, url: str) -> Optional[Rss]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM rss WHERE url = %s", (url,))
            res = cur.fetchone()
            if res is None:
                return None
            else:
                return Rss(*res[1:], _id=res[0])

    def insert_rss(self, e: Rss) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rss (url, ticker, fetched_at) VALUES (%s, %s, %s) RETURNING id;",
                (e.url, e.ticker, e.fetched_at)
                # astuple(e)
            )
            _id = cur.fetchone()[0]
            self.conn.commit()
        return _id

    def insert_article(self, e: Article):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rss (url, ticker, fetched_at) VALUES (%s, %s, %s) RETURNING id;",
                astuple(e)
            )
            self.conn.commit()

    def update_rss(self, id: int):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE rss SET fetched_at=now() WHERE ID=%s;", (id,))
            self.conn.commit()


def Conn(database):
    if database:
        print("[+] Inserting into Database: " + str(database))
        conn = init(database)
        if isinstance(conn, str):
            print(str)
            sys.exit(1)
    else:
        conn = ""

    return conn


def init(db):
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        table_users = """
            CREATE TABLE IF NOT EXISTS
                users(
                    id integer not null,
                    id_str text not null,
                    name text,
                    username text not null,
                    bio text,
                    location text,
                    url text,
                    join_date text not null,
                    join_time text not null,
                    tweets integer,
                    following integer,
                    followers integer,
                    likes integer,
                    media integer,
                    private integer not null,
                    verified integer not null,
                    profile_image_url text not null,
                    background_image text,
                    hex_dig  text not null,
                    time_update integer not null,
                    CONSTRAINT users_pk PRIMARY KEY (id, hex_dig)
                );
            """
        cursor.execute(table_users)

        table_tweets = """
            CREATE TABLE IF NOT EXISTS
                tweets (
                    id integer not null,
                    id_str text not null,
                    tweet text default '',
                    conversation_id text not null,
                    created_at integer not null,
                    date text not null,
                    time text not null,
                    timezone text not null,
                    place text default '',
                    replies_count integer,
                    likes_count integer,
                    retweets_count integer,
                    user_id integer not null,
                    user_id_str text not null,
                    screen_name text not null,
                    name text default '',
                    link text,
                    mentions text,
                    hashtags text,
                    cashtags text,
                    urls text,
                    photos text,
                    quote_url text,
                    video integer,
                    geo text,
                    near text,
                    source text,
                    time_update integer not null,
                    `translate` text default '',
                    trans_src text default '',
                    trans_dest text default '',
                    PRIMARY KEY (id)
                );
        """
        cursor.execute(table_tweets)

        table_retweets = """
            CREATE TABLE IF NOT EXISTS
                retweets(
                    user_id integer not null,
                    username text not null,
                    tweet_id integer not null,
                    retweet_id integer not null,
                    retweet_date integer not null,
                    CONSTRAINT retweets_pk PRIMARY KEY(user_id, tweet_id),
                    CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY(tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_retweets)

        table_reply_to = """
            CREATE TABLE IF NOT EXISTS
                replies(
                    tweet_id integer not null,
                    user_id integer not null,
                    username text not null,
                    CONSTRAINT replies_pk PRIMARY KEY (user_id, tweet_id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_reply_to)

        table_favorites = """
            CREATE TABLE IF NOT EXISTS
                favorites(
                    user_id integer not null,
                    tweet_id integer not null,
                    CONSTRAINT favorites_pk PRIMARY KEY (user_id, tweet_id),
                    CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_favorites)

        table_followers = """
            CREATE TABLE IF NOT EXISTS
                followers (
                    id integer not null,
                    follower_id integer not null,
                    CONSTRAINT followers_pk PRIMARY KEY (id, follower_id),
                    CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
                    CONSTRAINT follower_id_fk FOREIGN KEY(follower_id) REFERENCES users(id)
                );
        """
        cursor.execute(table_followers)

        table_following = """
            CREATE TABLE IF NOT EXISTS
                following (
                    id integer not null,
                    following_id integer not null,
                    CONSTRAINT following_pk PRIMARY KEY (id, following_id),
                    CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
                    CONSTRAINT following_id_fk FOREIGN KEY(following_id) REFERENCES users(id)
                );
        """
        cursor.execute(table_following)

        table_followers_names = """
            CREATE TABLE IF NOT EXISTS
                followers_names (
                    user text not null,
                    time_update integer not null,
                    follower text not null,
                    PRIMARY KEY (user, follower)
                );
        """
        cursor.execute(table_followers_names)

        table_following_names = """
            CREATE TABLE IF NOT EXISTS
                following_names (
                    user text not null,
                    time_update integer not null,
                    follows text not null,
                    PRIMARY KEY (user, follows)
                );
        """
        cursor.execute(table_following_names)

        return conn
    except Exception as e:
        return str(e)


def fTable(Followers):
    if Followers:
        table = "followers_names"
    else:
        table = "following_names"

    return table


def uTable(Followers):
    if Followers:
        table = "followers"
    else:
        table = "following"

    return table


def follow(conn, Username, Followers, User):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        entry = (User, time_ms, Username,)
        table = fTable(Followers)
        query = f"INSERT INTO {table} VALUES(?,?,?)"
        cursor.execute(query, entry)
        conn.commit()
    except sqlite3.IntegrityError:
        pass


def get_hash_id(conn, id):
    cursor = conn.cursor()
    cursor.execute('SELECT hex_dig FROM users WHERE id = ? LIMIT 1', (id,))
    resultset = cursor.fetchall()
    return resultset[0][0] if resultset else -1


def user(conn, config, User):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        user = [int(User.id), User.id, User.name, User.username, User.bio, User.location, User.url, User.join_date, User.join_time, User.tweets,
                User.following, User.followers, User.likes, User.media_count, User.is_private, User.is_verified, User.avatar, User.background_image]

        hex_dig = hashlib.sha256(','.join(str(v)
                                          for v in user).encode()).hexdigest()
        entry = tuple(user) + (hex_dig, time_ms,)
        old_hash = get_hash_id(conn, User.id)

        if old_hash == -1 or old_hash != hex_dig:
            query = f"INSERT INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            cursor.execute(query, entry)
        else:
            pass

        if config.Followers or config.Following:
            table = uTable(config.Followers)
            query = f"INSERT INTO {table} VALUES(?,?)"
            cursor.execute(query, (config.User_id, int(User.id)))

        conn.commit()
    except sqlite3.IntegrityError:
        pass


def tweets(conn, Tweet, config):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        entry = (Tweet.id,
                 Tweet.id_str,
                 Tweet.tweet,
                 Tweet.conversation_id,
                 Tweet.datetime,
                 Tweet.datestamp,
                 Tweet.timestamp,
                 Tweet.timezone,
                 Tweet.place,
                 Tweet.replies_count,
                 Tweet.likes_count,
                 Tweet.retweets_count,
                 Tweet.user_id,
                 Tweet.user_id_str,
                 Tweet.username,
                 Tweet.name,
                 Tweet.link,
                 ",".join(Tweet.mentions),
                 ",".join(Tweet.hashtags),
                 ",".join(Tweet.cashtags),
                 ",".join(Tweet.urls),
                 ",".join(Tweet.photos),
                 Tweet.quote_url,
                 Tweet.video,
                 Tweet.geo,
                 Tweet.near,
                 Tweet.source,
                 time_ms,
                 Tweet.translate,
                 Tweet.trans_src,
                 Tweet.trans_dest)
        cursor.execute(
            'INSERT INTO tweets VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', entry)

        if config.Favorites:
            query = 'INSERT INTO favorites VALUES(?,?)'
            cursor.execute(query, (config.User_id, Tweet.id))

        if Tweet.retweet:
            query = 'INSERT INTO retweets VALUES(?,?,?,?,?)'
            _d = datetime.timestamp(datetime.strptime(
                Tweet.retweet_date, "%Y-%m-%d %H:%M:%S"))
            cursor.execute(query, (int(Tweet.user_rt_id),
                                   Tweet.user_rt, Tweet.id, int(Tweet.retweet_id), _d))

        if Tweet.reply_to:
            for reply in Tweet.reply_to:
                query = 'INSERT INTO replies VALUES(?,?,?)'
                cursor.execute(query, (Tweet.id, int(
                    reply['user_id']), reply['username']))

        conn.commit()
    except sqlite3.IntegrityError:
        pass
