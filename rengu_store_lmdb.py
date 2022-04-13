import re
from collections.abc import Set
from fnmatch import fnmatchcase
from struct import pack, unpack
from uuid import UUID, uuid4

try:
    from orjson import dumps, loads
except:
    from json import dumps as _dumps, loads

    def dumps(obj):
        return _dumps(obj).encode()


import lmdb
from rengu.store import RenguStorageError, RenguStore

# Regex matches
RE_GLOB = re.compile(r"[\*\?\[]")


def pack_index(ID: UUID, index: int) -> str:
    return pack("16s1q", ID.bytes, index)


def unpack_index(value: str) -> tuple[UUID, int]:

    if len(value) == 16:
        return UUID(bytes=value), 0, 0, 0
    else:
        value, index = unpack("16s1q")
        return UUID(bytes=value), index


class RenguStoreLmdbRo(RenguStore):
    def __init__(self, name: str, extra: list[str]):

        path = name.split(":", 1)[1]

        self.db = lmdb.open(path, max_dbs=4, map_size=2**40 - 1)

        self.index_db = self.db.open_db(b"index", dupsort=True)
        self.data_db = self.db.open_db(b"data")

    class ResultSet(Set):
        def __init__(
            self,
            term: str,
            parent: "RenguStoreLmdbRo",
            txn: lmdb.Transaction,
            result: "ResultSet" = None,
        ):

            self.parent = parent
            self.term = term
            self.seen = set()
            self.seen_all = False
            self.seen_pos = 0
            self.txn = txn

            if result:
                self.seen_all = True
                self.seen = set(result)
                self.cursor = None
                return

            # Set the defaul start for the cursor
            self.start = self.term

            # If there is a glob, start at the glob beginning
            self.glob = False
            if m := RE_GLOB.search(self.term):
                self.glob = True
                self.start = self.start[: m.start()]

            # Get the cursor and set the range
            self.cursor = txn.cursor(self.parent.index_db)

            # If the range isn't in the DB, then we've seen all
            # and it is an empty set
            if not self.cursor.set_range(self.start.encode()):
                self.seen_all = True
                self.cursor.close()

        def __iter__(self):

            if self.seen_all:
                self.seen_pos = 0
            else:
                if not self.cursor.set_range(self.start.encode()):
                    self.seen_all = True
                    self.cursor.close()

            return self

        def __next__(self):

            if self.seen_all:

                if self.seen_pos >= len(self.seen):
                    raise StopIteration

                self.seen_pos += 1
                return list(self.seen)[self.seen_pos - 1]

            else:

                while key := self.cursor.key().decode():

                    if self.glob:

                        # Stop iteration of the start doesn't match
                        if not key[: len(self.start)] == self.start:
                            self.seen_all = True
                            raise StopIteration

                        # Skip if this doesn't match the glob
                        if not fnmatchcase(key, self.term):
                            self.cursor.next()
                            continue

                    # break if not a glob and prefix doesn't match
                    elif not key == self.start:
                        self.seen_all = True
                        raise StopIteration

                    # All other cases, get the UUID
                    try:
                        u = UUID(bytes=self.cursor.value())
                    except ValueError:
                        self.cursor.next()
                        continue

                    # Skip seen values
                    if u in self.seen:
                        self.cursor.next()
                        continue

                    # add to seen and yield
                    else:
                        self.seen.add(u)
                        self.cursor.next()
                        return u

            self.seen_all = True
            raise StopIteration

        def __contains__(self, i):

            # Shortcut for seen all
            if self.seen_all:
                return i in self.seen

            # iterate through the list
            for j in self:
                if i in j:
                    return True

            return False

        def __len__(self):

            # Shortcut for seen all
            if self.seen_all:
                return len(self.seen)

            return len(set(self))

        def __repr__(self):

            return f"{self.term}[n={len(self)}]"

        def __and__(self, other):
            _ = len(self) > len(other)
            return RenguStoreLmdbRo.ResultSet(
                f"( {self} & {other} )",
                self.parent,
                self.txn,
                result=self.seen & other.seen,
            )

        def __sub__(self, other):
            _ = len(self) > len(other)
            return RenguStoreLmdbRo.ResultSet(
                f"( {self} - {other} )",
                self.parent,
                self.txn,
                result=self.seen - other.seen,
            )

        def __or__(self, other):
            _ = len(self) > len(other)
            return RenguStoreLmdbRo.ResultSet(
                f"( {self} | {other} )",
                self.parent,
                self.txn,
                result=self.seen | other.seen,
            )

        def __xor__(self, other):
            _ = len(self) > len(other)
            return RenguStoreLmdbRo.ResultSet(
                f"( {self} ^ {other} )",
                self.parent,
                self.txn,
                result=self.seen ^ other.seen,
            )

    def __len__(self):
        txn = self.db.begin()
        cursor = txn.cursor(self.data_db)
        return len(list(cursor))

    def get(self, ID: UUID):
        """Get detail for the specified ID"""

        txn = self.db.begin()
        cursor = txn.cursor(self.data_db)
        obj = cursor.get(ID.bytes)
        if obj:
            return loads(obj)
        else:
            return {}

    def query(
        self, args: list[str], default_operator: str = "&", with_data: bool = False
    ):

        txn = self.db.begin()

        # last term is None
        args = [*args]
        args.append(None)

        def _parse(depth=0):

            current_operator = None
            result = None

            while q := args.pop(0):

                r = None

                # Switch operator
                if q in "&-|^":
                    current_operator = q
                    continue

                # Nest query
                if q == "(":
                    r = _parse(depth=depth + 1)

                elif q == ")":
                    if depth < 1:
                        raise RenguStorageError("Invalid subquery - unmatched )")
                    return result

                # special case of Glob all
                elif q == "*":
                    r = self.ResultSet("ID=*", self, txn)

                # standard resultset
                else:
                    r = self.ResultSet(q, self, txn)

                # Run the query operation
                if not current_operator:
                    result = r

                elif current_operator == "&":
                    result = result & r

                elif current_operator == "-":
                    result = result - r

                elif current_operator == "|":
                    result = result | r

                elif current_operator == "^":
                    result = result ^ r

                else:
                    raise RenguStorageError("No operator specified")

                current_operator = default_operator

            return result

        return _parse()


class RenguStoreLmdbRw(RenguStoreLmdbRo):
    def delete(self, ID: UUID):

        with self.db.begin(write=True, db=self.data_db) as data_txn:

            obj = self.get(ID)

            with self.db.begin(
                write=True, db=self.index_db, parent=data_txn
            ) as index_txn:

                # Delete regardless of index
                for term, _ in self.index(obj):
                    term = term[:255].encode()

                    index_txn.delete(term, ID.bytes)

            data_txn.delete(ID.bytes)

    def save(self, obj: dict) -> UUID:

        if not self.validate(obj):
            raise RenguStorageError("Invalid rengu data")

        # Set new UUID if none exists
        ID = UUID(obj.get("ID")) or uuid4()

        with self.db.begin(write=True, db=self.data_db) as data_txn:

            data_txn.put(ID.bytes, dumps(dict(obj)))

            with self.db.begin(
                write=True, db=self.index_db, parent=data_txn
            ) as index_txn:

                for term, index in self.index(obj):

                    term = term[:255].encode()
                    try:
                        index_txn.put(term, ID.bytes, dupdata=True)
                    except lmdb.BadValsizeError:
                        print(f"Invalid term {term} in {ID}")
                        raise
        return ID
