import MySQLdb
import traceback
def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL',
        '_log_type': 'Database'
    }

'''
Generic SQL insertion function
'''
def insert_row(db_cursor, query, entity, objType, logger):
    num_inserted = 0
    row_id = 0

    try:
        db_cursor.execute(query)
        num_inserted = db_cursor.rowcount
        row_id = db_cursor.lastrowid
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload(objType, (query%entity)))

    return num_inserted, row_id


'''
Generic SQL selection functions
'''
def is_entity_in_db(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        query = db_cursor.fetchone()
        if query is not None:
            return query[0]
    except:
        logger.warning('Check Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload(objType, (query%entity)))
    return False

def insert_entity(db_cursor, entity, qs_query, qi_query, objType, logger):
    if not is_entity_in_db(db_cursor, qs_query, entity, objType, logger):
        try:
            db_cursor.execute(qi_query, entity)
            return db_cursor.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload(objType, (qi_query%entity)))
    return 0


def get_entity_id(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        if db_cursor.rowcount == 1:
            return db_cursor.fetchone()[0]
        else:
            print('Error selecting entity with query ' + query)
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                           additional_fields=create_payload(objType, (query % entity)))

    return None

'''
Generic SQL update function
'''
def update_entity(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        return db_cursor.rowcount
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload(objType, (query % entity)))
    return 0