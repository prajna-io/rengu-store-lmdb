#!/usr/bin/env python

import sys
from json import loads
from os import environ
from pprint import pprint

from rengu_store_lmdb import RenguStoreLmdbRo, unpack_index

store = RenguStoreLmdbRo(name=environ["RENGU_BASE"], extra=[])

for ID in store.query(sys.argv[1:]):

    with store.db.begin() as data_txn:
        cursor = data_txn.cursor(store.data_db)

        data = loads(cursor.get(ID.bytes))

        pprint(data)

    with store.db.begin(db=store.index_db) as index_txn:
        index = {
            key.decode(): unpack_index(value)
            for key, value in index_txn.cursor()
            if value.startswith(ID.bytes)
        }
        pprint(index)
