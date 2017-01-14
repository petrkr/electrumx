# Copyright (c) 2016-2017, the ElectrumX authors
#
# All rights reserved.
#
# See the file "LICENCE" for information about the copyright
# and warranty status of this software.

'''Backend database abstraction.'''

import os
from functools import partial

import lib.util as util


def db_class(name):
    '''Returns a DB engine class.'''
    for db_class in util.subclasses(Storage):
        if db_class.__name__.lower() == name.lower():
            db_class.import_module()
            return db_class
    raise RuntimeError('unrecognised DB engine "{}"'.format(name))


class ReadBatch(object):
    '''Abstract base class representing a batch.  Must support the
    context manager protocol.
    '''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def get(self, key):
        '''Read the binary key from the DB and return the binary value, or
        None if it is not present.
        '''
        raise NotImplementedError

    def iterator(self, prefix=b'', reverse=False):
        '''Return an iterator that yields (key, value) pairs from the
        database sorted by key.

        Only keys starting with the given prefix are returned.
        If reverse is True the items are returned in reverse order.
        '''
        raise NotImplementedError


class WriteBatch(ReadBatch):
    '''Abstract base class representing a read and write batch.  Must
    support the context manager protocol.

    Writes must only be committed when the context manager closes
    without an exception.
    '''

    def put(self, key, value):
        '''Write the binary key to the DB with the binary value.  Overwrite
        any existing key.'''
        raise NotImplementedError

    def delete(self, key):
        '''Delete the binary key from the DB.  Do nothing if it is not
        present.'''
        raise NotImplementedError


class Storage(object):
    '''Abstract base class of the DB backend abstraction.'''

    def __init__(self, name, for_sync):
        self.is_new = not os.path.exists(name)
        self.for_sync = for_sync or self.is_new
        self.open(name, create=self.is_new)

    @classmethod
    def import_module(cls):
        '''Import the DB engine module.'''
        raise NotImplementedError

    def open(self, name, create):
        '''Open an existing database or create a new one.'''
        raise NotImplementedError

    def close(self):
        '''Close the database.'''
        raise NotImplementedError

    def read_batch(self):
        '''Returns a read-only batch that supports the context manager
        protocol.  The read batch should maintain a consistent view of
        the database.
        '''
        raise NotImplementedError

    def write_batch(self):
        '''Returns a batch that supports the context manager protocol.

        If write is True, changes are only committed if the context is
        exited cleanly.

        Note: It is unspecified whether writes (puts and deletes) are
        visible by subsequent gets or iterator values in the same batch.
        '''
        raise NotImplementedError


class LevelDB(Storage):
    '''LevelDB database engine.'''

    @classmethod
    def import_module(cls):
        import plyvel
        cls.module = plyvel

    def open(self, name, create):
        mof = 512 if self.for_sync else 128
        self.db = self.module.DB(name, create_if_missing=create,
                                 max_open_files=mof, compression=None)

    def close(self):
        self.db.close()

    def read_batch(self):
        # Could use "self.db.snapshot()" but it seems slightly slower
        return LevelDBReadBatch(self.db)

    def write_batch(self):
        return LevelDBWriteBatch(self.db)


class LevelDBReadBatch(ReadBatch):

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db


class LevelDBWriteBatch(WriteBatch):

    def __init__(self, db):
        batch = db.write_batch(transaction=True, sync=True)
        self.batch = batch
        self.get = db.get
        self.iterator = db.iterator
        self.put = batch.put
        self.delete = batch.delete

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self.batch.write()
        return False


class RocksDB(Storage):
    '''RocksDB database engine.'''

    @classmethod
    def import_module(cls):
        import rocksdb
        cls.module = rocksdb

    def open(self, name, create):
        mof = 512 if self.for_sync else 128
        compression = "no"
        compression = getattr(self.module.CompressionType,
                              compression + "_compression")
        options = self.module.Options(create_if_missing=create,
                                      compression=compression,
                                      use_fsync=True,
                                      target_file_size_base=33554432,
                                      max_open_files=mof)
        self.db = self.module.DB(name, options)

    def close(self):
        # PyRocksDB doesn't provide a close method; hopefully this is enough
        self.db = None
        import gc
        gc.collect()

    def read_batch(self):
        return RocksDBReadBatch(self.db)

    def write_batch(self):
        return RocksDBWriteBatch(self.db)


class RocksDBReadBatch(ReadBatch):

    def __init__(self, db):
        self.get = db.get
        self.iterator = partial(RocksDBIterator, db)


class RocksDBWriteBatch(WriteBatch):

    def __init__(self, db):
        batch = db.write_batch(transaction=True, sync=True)
        self.batch = batch
        self.get = db.get
        self.iterator = db.iterator
        self.put = batch.put
        self.delete = batch.delete

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self.batch.write()
        return False


class RocksDBIterator(object):
    '''A RocksDB iterator.  Similar in style to LMDBIterator.'''

    def __init__(self, db, prefix=b'', reverse=False):
        self.prefix = prefix
        if reverse:
            self.iterator = reversed(db.iteritems())
            nxt_prefix = util.increment_byte_string(prefix)
            if nxt_prefix:
                self.iterator.seek(nxt_prefix)
                try:
                    next(self.iterator)
                except StopIteration:
                    self.iterator.seek(nxt_prefix)
            else:
                self.iterator.seek_to_last()
        else:
            self.iterator = db.iteritems()
            self.iterator.seek(prefix)

    def __iter__(self):
        return self

    def __next__(self):
        k, v = next(self.iterator)
        if not k.startswith(self.prefix):
            raise StopIteration
        return k, v


class LMDB(Storage):
    '''LMDB database engine.'''

    @classmethod
    def import_module(cls):
        import lmdb
        cls.module = lmdb

    def open(self, name, create):
        # I don't see anything equivalent to max_open_files for for_sync
        self.env = LMDB.module.Environment(name, subdir=True, create=create,
                                           max_dbs=0, map_size=5 * 10 ** 10)

    def close(self):
        self.env.close()

    def read_batch(self):
        return LMDBReadBatch(self.env)

    def write_batch(self):
        return LMDBWriteBatch(self.env)


class LMDBReadBatch(ReadBatch):

    def __init__(self, env):
        txn = env.begin()
        self.get = txn.get
        self.iterator = partial(LMDBIterator, txn)


class LMDBWriteBatch(WriteBatch):

    def __init__(self, env):
        txn = env.begin(write=True)
        self.txn = txn
        self.get = txn.get
        self.iterator = partial(LMDBIterator, txn)
        self.put = txn.put
        self.delete = txn.delete

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self.txn.commit()
        return False


class LMDBIterator(object):

    def __init__(self, txn, prefix=b'', reverse=False):
        self.prefix = prefix
        self.cursor = txn.cursor()
        if reverse:
            nxt_prefix = util.increment_byte_string(prefix)
            if nxt_prefix:
                self.cursor.set_range(nxt_prefix)
            # This will position at end of DB end if not nxt_prefix
            self.iterator = self.cursor.iterprev()
            try:
                next(self.iterator)
            except StopIteration:
                self.iterator = self.cursor.iterprev()
        else:
            self.cursor.set_range(self.prefix)
            self.iterator = self.cursor.iternext()

    def __iter__(self):
        return self

    def __next__(self):
        k, v = next(self.iterator)
        if not k.startswith(self.prefix):
            raise StopIteration
        return k, v
