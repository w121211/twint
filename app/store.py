import datetime

from peewee import Model, PostgresqlDatabase, CharField, TextField, IntegerField, DateTimeField, ForeignKeyField, CompositeKey, SmallIntegerField
# from playhouse.postgres_ext import JSONField

# db = PostgresqlDatabase('my_database', user='postgres')
db = PostgresqlDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db


class Rss(BaseModel):
    url = CharField(unique=True)
    ticker = CharField(null=True)
    freq = IntegerField(default=2*24*60*60)  # seconds (default: 2 days)
    n_retries = SmallIntegerField(default=0)
    fetched_at = DateTimeField(null=True)


class RssShot(BaseModel):
    url = CharField(unique=True)
    ticker = CharField(null=True)
    entries = TextField()


class Page(BaseModel):
    url = CharField(unique=True)
    rss_title = TextField(null=True)
    rss_summary = TextField(null=True)
    rss_published_at = DateTimeField(null=True)
    # rss_tickers = JSONField()
    rss_tickers = TextField(default="")
    parsed_title = TextField(null=True)
    parsed_text = TextField(null=True)
    parsed_published_at = DateTimeField(null=True)
    parsed_tickers = TextField(null=True)
    raw = TextField(null=True)
    n_retries = SmallIntegerField(default=0)
    fetched_at = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)


# class EntryToPage(BaseModel):
#     entry = ForeignKeyField(Entry)
#     page = ForeignKeyField(Page)

#     class Meta:
#         primary_key = CompositeKey('entry', 'page')


def create_tables():
    with db:
        # db.drop_tables([Entry, Page, EntryToPage])
        db.create_tables([Rss, RssShot])


def seed():
    with db:
        db.drop_tables([Rss, RssShot])
        db.create_tables([Rss, RssShot])

    # entries = [
    #     {"url": "http://aaa.com", 'last_published_at': datetime.datetime.now()},
    #     # {"url": "http://bbb.com"},
    # ]

    # with db.atomic():
    #     for d in entries:
    #         e = Entry.create(**d)
    #         print(e.last_published_at, e.fetched_at)
