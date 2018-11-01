

#Table Created on HashDB

# CREATE TABLE IF NOT EXISTS FL_Hearings
# (
#     hash varchar(64) PRIMARY KEY NOT NULL,
#     fileName varchar(64),
#     lastTouched timestamp DEFAULT NOW() ON UPDATE NOW()
# );
# ENGINE = INNODB
# CHARACTER SET utf8 COLLATE utf8_general_ci;

from Utils.Generic_MySQL import *
from hashlib import blake2b

# the mysql execute function will not accept variable table names, hence the `{0}` pre-formatting
GET_OLD = '''SELECT hash FROM {0} WHERE hash = %(new_hash)s'''
ADD_NEW = '''INSERT INTO {0} (hash, fileName) VALUE (%(hash)s, %(f_name)s)'''

REMOVE_OLD = '''DELETE FROM {0} WHERE hash = %(hash)s'''


class KnownFileComparator():
    def __init__(self, hash_DB, logger):
        self.hash_DB = hash_DB
        self.logger = logger

    def is_new(self, table_name, f_name):
        with open(f_name, "r") as f:
            new_hash = self.get_hash(f)
            f.close()
            get_entity_id(self.hash_DB,
                          GET_OLD.format(table_name),
                          {'new_hash': new_hash},
                          "Hash Record",
                          self.logger)

            return self.hash_DB.rowcount == 0

    def get_hash(self, f):
        h = blake2b(digest_size=32)
        h.update(f.read().encode("utf-8"))
        return h.hexdigest()

    def add_file_hash(self, table_name, f_name):
        with open(f_name, "r") as f:
            file_hash = self.get_hash(f)

            insert_entity(self.hash_DB,
                          {'f_name': f_name, 'hash': file_hash},
                          ADD_NEW.format(table_name),
                          "Hash Record",
                          self.logger)
            # self.hash_DB.commit()
            f.close()

    def remove_file_hash(self, table_name, f_name):
        with open(f_name, "r") as f:
            file_hash = self.get_hash(f)
            f.close()
            remove_entity(self.hash_DB,
                          {'f_name': f_name, 'hash': file_hash},
                          REMOVE_OLD.format(table_name),
                          "Hash Record",
                          self.logger)







