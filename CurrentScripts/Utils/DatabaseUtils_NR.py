import MySQLdb
import traceback

def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL',
        '_log_type': 'Database'
    }

def is_obj_in_db(dddb, query, obj, objType, logger):
    try:
        print((query%obj))
        dddb.execute(query, obj)
        query = dddb.fetchone()
        print("query: "  + str(query))
        if query is not None:
            return query[0]
    except:
        logger.warning('Check Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload(objType, (query%obj)))
    return False

def insert_obj(dddb, obj, qs_query, qi_query, objType, logger):
    if not is_obj_in_db(dddb, qs_query, obj, objType, logger):
        try:
            print("\n\n\n HERE")
            dddb.execute(qi_query, obj)
            return dddb.rowcount
        except MySQLdb.Error:
            print("\n\nfdasdf")
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload(objType, (qi_query%obj)))
    return 0