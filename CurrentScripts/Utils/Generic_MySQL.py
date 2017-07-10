import traceback
from Constants.Committee_Queries import SELECT_SESSION_YEAR
from Database_Connection import *

def create_payload(table, sqlstmt, state = "N/A"):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': state,
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

def insert_entity_with_check(db_cursor, entity, qs_query, qi_query, objType, logger):
    if not is_entity_in_db(db_cursor, qs_query, entity, objType, logger):
        return insert_entity(db_cursor, entity, qi_query, objType, logger)
    return False

def insert_entity(db_cursor, entity, qi_query, objType, logger):
    try:
        db_cursor.execute(qi_query, entity)
        return int(db_cursor.lastrowid)
    except MySQLdb.Error:
        logger.warning('Insert Failed for ' + objType, full_msg=traceback.format_exc(),
                additional_fields=create_payload(objType, (qi_query%entity)))
    return False


def get_entity_id(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        if db_cursor.rowcount == 1:
            return db_cursor.fetchone()[0]
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                           additional_fields=create_payload(objType, (query % entity)))

    return False

def get_all(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)

        return db_cursor.fetchall()
    except MySQLdb.Error:
        logger.warning("Failed Select All", full_msg=traceback.format_exc(),
                       additional_fields=create_payload(objType, (query % entity)))

    return False
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
    return False

def get_session_year(db_cursor, state, logger):
        entity = {"state" : state}
        return is_entity_in_db(db_cursor=db_cursor,
                                query=SELECT_SESSION_YEAR,
                                entity=entity,
                                objType="Session for State",
                                logger=logger)