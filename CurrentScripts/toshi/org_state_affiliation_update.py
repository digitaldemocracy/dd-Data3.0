import MySQLdb
from queries import org_update
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect

def run(db_cursor, query, logger):
    try:
        db_cursor.execute(query)
    except MySQLdb.Error:
        logger.exception("Query Failed: " + query)
        return False

    return True


def main():
    with connect() as dddb:
        logger = create_logger()
        run(dddb, org_update, logger)


if __name__ == "__main__":
    main()
