import mysql.connector
from mysql.connector import errorcode
from Utils.Generic_Utils import create_logger

class MySQL_Wrapper(object):
    def __init__(self, host, database, user, password, logger):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.logger = logger
        self.config = {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'database': self.database,
            'raise_on_warnings': True,
            'buffered':True,
            'use_unicode': True
        }
        self.connection = self.connect(**self.config)
        self.analytics = dict()


    def __enter__(self):
        '''
        Context manager enter. Allows class to be used with the "with" keyword
        :return: self
        '''
        if self.connection.autocommit:
            self.connection.autocommit = False
        return self.connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        Context manager exit. Allows for safe exit from connection. Ensures cursor and db
        connection close if/when script exits.
        :param exc_type: Exception type.
        :param exc_val: Exception value.
        :param exc_tb: Exception Table.
        :return: None
        '''
        if exc_type:
            self.logger.exception("MySQL Error: " + str(exc_type) + "\nTable: " + str(exc_tb) + "\nMessage: " + str(exc_val))
            self.close()
        else:
            self.connection.commit()

    def close(self):
        '''
        Checks if there is an active cursor and connection and closes them.
        :return:
        '''
        if hasattr(self, "cursor"):
            self.cursor.close()
        if hasattr(self, "connection"):
            self.connection.rollback()
            self.connection.close()

    def connect(self, **new_config):
        '''
        If there is an existing connection rollback any uncommitted queries and create a new connection.
        :param new_config: mysql.connector parameters. Reference documentation for long list.
        :return: The mysql connection
        '''
        self.close()
        return mysql.connector.connect(**new_config)



    # def is_entity_in_db(self, db_cursor, query, entity, objType, logger):
    #     try:
    #         db_cursor.execute(query, entity)
    #         query = db_cursor.fetchone()
    #         if query is not None:
    #             return query[0]
    #     except MySQLdb.Error:
    #         logger.exception(format_logger_message('Check Failed for ' + objType, (query % entity)))
    #
    #     return False
    #
    # def insert_entity_with_check(self, db_cursor, entity, qs_query, qi_query, objType, logger):
    #     result = is_entity_in_db(self, db_cursor, qs_query, entity, objType, logger)
    #     if not result:
    #         return insert_entity(self, db_cursor, entity, qi_query, objType, logger)
    #     return result
    #
    # def insert_entity(self, db_cursor, entity, qi_query, objType, logger):
    #     try:
    #         db_cursor.execute(qi_query, entity)
    #         return int(self, db_cursor.lastrowid)
    #     except MySQLdb.Error:
    #         logger.exception(format_logger_message('Insert Failed for ' + objType, (qi_query % entity)))
    #
    #     return False
    #
    # def get_entity_id(self, db_cursor, query, entity, objType, logger):
    #     try:
    #         db_cursor.execute(query, entity)
    #         if db_cursor.rowcount == 1:
    #             return db_cursor.fetchone()[0]
    #     except MySQLdb.Error:
    #         logger.exception(format_logger_message('ID Retrieval Failed for ' + objType, (query % entity)))
    #     return False
    #
    # def get_entity(self, db_cursor, query, entity, objType, logger):
    #     try:
    #         db_cursor.execute(query, entity)
    #         if db_cursor.rowcount == 1:
    #             return db_cursor.fetchone()
    #     except MySQLdb.Error:
    #         logger.exception(format_logger_message('ID Retrieval Failed for ' + objType, (query % entity)))
    #     return False
    #
    # def get_all(self, db_cursor, query, entity, objType, logger):
    #     try:
    #         db_cursor.execute(query, entity)
    #         return db_cursor.fetchall()
    #     except MySQLdb.Error:
    #         logger.exception(format_logger_message('Failed Selecting All for ' + objType, (query % entity)))
    #     return False
    #
    # '''
    # Generic SQL update function
    # '''
    #
    # def update_entity(self, db_cursor, query, entity, objType, logger):
    #     try:
    #         db_cursor.execute(query, entity)
    #         return db_cursor.rowcount
    #     except:
    #         logger.exception(format_logger_message('Update Failed for ' + objType, (query % entity)))
    #
    #     return False
    #
    # def get_session_year(self, db_cursor, state, logger):
    #     entity = {"state": state}
    #     return is_entity_in_db(self, db_cursor=db_cursor,
    #                            query=SELECT_SESSION_YEAR,
    #                            entity=entity,
    #                            objType="Session for State",
    #                            logger=logger)
    #
    # def get_pid(dddb, logger, person, source_link=None):
    #     '''
    #     Given a committee member, use the given fields to find the pid.
    #     Cases:
    #             1. OpenStates does not provide an altId or an incorrect altId.'
    #             2. Information is scraped from CA committee websites.
    #     :param person: A CommitteeMember model object and the committee they belong to.
    #     :param source_link: A link to the source of the data
    #     :return: A pid if the CommitteeMember was found, false otherwise.
    #     '''
    #     if person.district:
    #         query = SELECT_LEG_WITH_HOUSE_DISTRICT
    #     elif person.house:
    #         query = SELECT_LEG_WITH_HOUSE
    #     else:
    #         query = SELECT_LEG_FIRSTLAST
    #
    #     pid_year_tuple = get_entity(self, db_cursor=dddb,
    #                                 entity=person.__dict__,
    #                                 query=query,
    #                                 objType="Get PID",
    #                                 logger=logger)
    #     if not pid_year_tuple:
    #         if person.district:
    #             query = SELECT_LEG_WITH_HOUSE_DISTRICT_LASTNAME
    #         elif person.house:
    #             query = SELECT_LEG_WITH_HOUSE_LASTNAME
    #         else:
    #             query = SELECT_LEG_LASTNAME
    #
    #         pid_year_tuple = get_entity(self, db_cursor=dddb,
    #                                     entity=person.__dict__,
    #                                     query=query,
    #                                     objType="Get Pid",
    #                                     logger=logger)
    #         if pid_year_tuple:
    #             vals = {"pid": pid_year_tuple[0], "name": person.alternate_name, "source": source_link}
    #             insert_entity(self, db_cursor=dddb,
    #                           entity=vals,
    #                           qi_query=INSERT_ALTERNATE_NAME,
    #                           objType="Alternate Name",
    #                           logger=logger)
    #         else:
    #             if person.district:
    #                 pid_year_tuple = get_entity(self, db_cursor=dddb,
    #                                             entity=person.__dict__,
    #                                             query=SELECT_LEG_WITH_HOUSE_DISTRICT_NO_NAME,
    #                                             objType="Get pid",
    #                                             logger=logger)
    #                 if pid_year_tuple:
    #                     logger.exception("Legislature found without name: " + str(person.__dict__))
    #                     vals = {"pid": pid_year_tuple[0], "name": person.alternate_name, "source": source_link}
    #                     insert_entity(self, db_cursor=dddb,
    #                                   entity=vals,
    #                                   qi_query=INSERT_ALTERNATE_NAME,
    #                                   objType="Alternate Name",
    #                                   logger=logger)
    #     return pid_year_tuple




