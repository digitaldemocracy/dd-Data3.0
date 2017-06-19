import MySQLdb
import traceback

'''
Unified SQL insertion function
'''
def insert_row(query, db):
    num_inserted = 0
    row_id = 0

    try:
        db.execute(query)

        num_inserted = db.rowcount
        row_id = db.lastrowid
    except MySQLdb.Error:
        #print('SQL insert failed for query ' + query)
        print(traceback.format_exc())

    return num_inserted, row_id


'''
Unified SQL selection function
'''
def is_entity_in_db(query, db):
    try:
        db.execute(query)

        if db.rowcount == 0:
            return False
        else:
            return True
    except MySQLdb.Error:
        #print('SQL select failed for query ' + query)
        print(traceback.format_exc())

    return None


def get_entity_id(query, db):
    try:
        db.execute(query)

        if db.rowcount == 1:
            return db.fetchone()[0]
        else:
            print('Error selecting entity with query ' + query)
    except MySQLdb.Error:
        #print('SQL select failed for query ' + query)
        print(traceback.format_exc())

    return None

'''
Unified SQL update function
'''
def update_entity(query, db):
    try:
        db.execute(query)
        return db.rowcount

    except MySQLdb.Error:
        #print('SQL update failed for query ' + query)
        print(traceback.format_exc())

    return 0