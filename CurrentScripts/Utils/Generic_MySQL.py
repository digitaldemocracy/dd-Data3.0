import sys
import MySQLdb
import re
from Constants.Find_Person_Queries import *
from Utils.Generic_Utils import format_logger_message
from Utils.Generic_Utils import levenshteinDistance, move_to_error_folder
from Constants.Committee_Queries import *

# //reload(sys)

# sys.setdefaultencoding('utf-8')

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
        logger.exception(format_logger_message('Insert Failed for ' + objType, (query%entity)))

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
    except MySQLdb.Error:
        logger.exception(format_logger_message('Check Failed for ' + objType, (query%entity)))

    return False

def insert_entity_with_check(db_cursor, entity, select_query, insert_query, objType, logger):
    result = is_entity_in_db(db_cursor, select_query, entity, objType, logger)
    if not result:
        return insert_entity(db_cursor, entity, insert_query, objType, logger)
    return result

def insert_entity(db_cursor, entity, insert_query, objType, logger):
    try:
        db_cursor.execute(insert_query, entity)
        return int(db_cursor.lastrowid)
    except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for ' + objType, (insert_query % entity)))

    return False

def remove_entity(db_cursor, entity, remove_query, objType, logger):
    try:
        db_cursor.execute(remove_query, entity)
        return True
    except MySQLdb.Error:
        logger.exception(format_logger_message('Removal Failed for ' + objType, (remove_query % entity)))
    return False

def get_entity_id(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        if db_cursor.rowcount == 1:
            return db_cursor.fetchone()[0]
        elif db_cursor.rowcount > 1:
            logger.exception(format_logger_message('ID Retrieval returned multiple rows ' +
                                                   objType + " " + str(entity), (query%entity)))
    except MySQLdb.Error:
        logger.exception(format_logger_message('ID Retrieval Failed for ' + objType, (query%entity)))
    return False

def get_entity(db_cursor, query, entity, objType, logger):
    try:

        db_cursor.execute(query, entity)
        if db_cursor.rowcount == 1:
            return db_cursor.fetchone()
    except MySQLdb.Error:
        logger.exception(format_logger_message('ID Retrieval Failed for ' + objType, (query % entity)))
    return False

def get_all(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        return db_cursor.fetchall()
    except MySQLdb.Error:
        logger.exception(format_logger_message('Failed Selecting All for ' + objType, (query%entity)))
    return False
'''
Generic SQL update function
'''
def update_entity(db_cursor, query, entity, objType, logger):
    try:
        db_cursor.execute(query, entity)
        return db_cursor.rowcount
    except:
        logger.exception(format_logger_message('Update Failed for ' + objType, (query%entity)))

    return False

def get_session_year(db_cursor, state, logger, legislator = False):
        entity = {"state" : state}
        query = SELECT_SESSION_YEAR
        if legislator:
            query = SELECT_SESSION_YEAR_LEGISLATOR
        return is_entity_in_db(db_cursor=db_cursor,
                                query=query,
                                entity=entity,
                                objType="Session for State",
                                logger=logger)

'''
Gets CID from our database using the committee names listed in the agendas
'''
def get_comm_cid(dddb_cursor, comm_name, house, session_year, state, logger, source_file=None):
    committee_info = {"name": comm_name, "house": house,
                      "session_year": session_year, "state": state}

    try:
        # Try exact match

        dddb_cursor.execute(SELECT_COMMITTEE_SHORT_NAME, committee_info)
        if dddb_cursor.rowcount == 1:
            return dddb_cursor.fetchone()[0]

        # Try alternate names match
        dddb_cursor.execute(SELECT_COMMITTEE_ALTERNATE_NAME, committee_info)

        if dddb_cursor.rowcount > 0:
            return min(dddb_cursor.fetchall(), key=lambda com: levenshteinDistance(com[1], comm_name))[0]

        # Try like match

        dddb_cursor.execute(SELECT_COMMITTEE_LIKE_SHORT_NAME, committee_info)

        if dddb_cursor.rowcount > 0:
            return min(dddb_cursor.fetchall(), key=lambda com: levenshteinDistance(com[1], comm_name))[0]

        # Try like match, replace punctuation and whitespace

        committee_info['name'] = ("%" + re.sub(r"[\W\s]+", "%", committee_info['name']) + "%").lower().replace("s%","%")
        dddb_cursor.execute(SELECT_COMMITTEE_LIKE_SHORT_NAME, committee_info)

        if dddb_cursor.rowcount > 0:
            return min(dddb_cursor.fetchall(), key=lambda com: levenshteinDistance(com[1], comm_name))[0]

        # Try like match, replace punctuation and whitespace and "and"

        committee_info['name'] = committee_info['name'].replace("%and%", "%")
        dddb_cursor.execute(SELECT_COMMITTEE_LIKE_SHORT_NAME, committee_info)

        if dddb_cursor.rowcount > 0:
            return min(dddb_cursor.fetchall(), key=lambda com: levenshteinDistance(com[1], comm_name))[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message("Committee selection failed for Committee ",
                                               (SELECT_COMMITTEE % committee_info)))
        if source_file is not None:
            move_to_error_folder(source_file)



def get_pid(dddb, logger, person, source_link=None, strict=False):
    '''
    Given a committee member or legislator model object,
    use the given fields to find the pid.
    Cases:
            1. OpenStates does not provide an altId or an incorrect altId.
            2. Information is scraped from CA committee websites.
            3. Finding a legislator from openstates data.
    :param person: A CommitteeMember model object and the committee they belong to.
    :param source_link: A link to the source of the data
    :return: A pid if the CommitteeMember was found, false otherwise.
    '''
    if person.district:
        query = SELECT_LEG_WITH_HOUSE_DISTRICT
    elif person.house:
        query = SELECT_LEG_WITH_HOUSE
    else:
       query = SELECT_LEG_FIRSTLAST

    pid = get_entity_id(db_cursor=dddb,
                                    entity=person.__dict__,
                                    query=query,
                                    objType="Get PID",
                                    logger=logger)
    if not pid:
        if person.district:
            query = SELECT_LEG_WITH_HOUSE_DISTRICT_LASTNAME
        elif person.house:
            query = SELECT_LEG_WITH_HOUSE_LASTNAME
        else:
            query = SELECT_LEG_LASTNAME

        pid = get_entity(db_cursor=dddb,
                            entity=person.__dict__,
                            query=query,
                            objType="Get Pid",
                            logger=logger)
        if pid:
            vals = {"pid" : pid, "name" : person.alternate_name, "source" : source_link}
            insert_entity(db_cursor=dddb, entity=vals, insert_query=INSERT_ALTERNATE_NAME, objType="Alternate Name",
                          logger=logger)
        # Should only be used for first time use. Problem is with re election
        # Finds district the maps new person to old person.
        #else:
        #    if person.district:
        #        pid_year_tuple = get_entity(db_cursor=dddb,
        #                            entity=person.__dict__,
        #                            query=SELECT_LEG_WITH_HOUSE_DISTRICT_NO_NAME,
        #                            objType="Get pid",
        #                            logger=logger)
        #        if pid_year_tuple:
        #            logger.exception("Legislature found without name: " + str(person.__dict__))
        #            vals = {"pid": pid_year_tuple[0], "name": person.alternate_name, "source": source_link}
        #            insert_entity(db_cursor=dddb,
        #                          entity=vals,
        #                          qi_query=INSERT_ALTERNATE_NAME,
        #                          objType="Alternate Name",
        #                          logger=logger)
    return pid
