import os
import sys
import logging
import datetime as dt

def capublic_format_committee_name(short_name, house):
    if house == 'CX' or house == "Assembly":
        return "Assembly Standing Committee on " + short_name
    return "Senate Standing Committee on " + short_name

def capublic_format_house(house):
    if house == "CX":
        return "Assembly"
    return "Senate"

def format_logger_message(subject, sql_statement):
    return "\n\t\t\t{\n\t\t\t\"Subject\": \"" + subject + "\"," \
           "\n\t\t\t\"SQL\": \"" + sql_statement + "\"\n\t\t\t}"
def format_end_log(subject, full_msg, additional_fields):
    return "\n\t\t\t{\n\t\t\t\"Subject\": \"" + subject + "\"," \
           "\n\t\t\t\"Message\": \"" + full_msg + "\""\
           "\n\t\t\t\"Message\": \"" + additional_fields + "\"\n\t\t\t}"
def create_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")
    filename = "logs/" + str(sys.argv[0].split("/")[-1]) + "_" + str(dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")) + ".log"
    # create logger
    logger = logging.getLogger("DDDB_Logger")
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("{\n\"Time\": \"%(asctime)s\","
                                  "\n\"File\": \"%(filename)s\","
                                  "\n\"Function\": \"%(funcName)s\","
                                  "\n\"Type\": \"%(levelname)s\","
                                  "\n\t\"Message\": %(message)s\n}")

    # # add formatter to ch
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger