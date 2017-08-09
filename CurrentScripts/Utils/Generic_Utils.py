#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
import os
import re
import sys
import logging
import datetime as dt

# reload(sys)
# sys.setdefaultencoding('utf8')

def format_committee_name(short_name, house, type, parent = None):
    house_formatted = "Assembly" if house == "CX" or house.lower() == "assembly" else "Senate"
    if "sub" in type.lower():
        return house_formatted + " " + parent + " " + short_name
    elif "legislative" in short_name.lower() and "joint" in type.lower() and "committee" not in short_name.lower():
        return short_name + " Committee"
    elif "joint" in type.lower():
        return short_name
    elif "other" in type.lower():
        return house_formatted + " Committee on " + short_name

    return house_formatted + " " + type + " Committee on " + short_name

def capublic_format_house(house):
    if house == "CX":
        return "Assembly"
    return "Senate"

def pdf_to_text_path():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return "/".join(dir_path.split("/")[:-1]) + "/pdftotext"

def format_absolute_path(relative_path):
    """
    Turns a relative path (starting from the CurrentScripts directory) into an absolute path
    by prepending an environment variable containing the location of a user's CurrentScripts directory
    :param relative_path: The relative path of a file or directory, in relation to the CurrentScripts directory
    :return: The absolute path of a file or directory
    """
    script_path = os.environ['SCRIPTPATH']

    absolute_path = script_path + relative_path

    return absolute_path


def format_logger_message(subject, sql_statement):
    return "\n\t\t\t{\n\t\t\t\"Subject\": \"" + subject + "\"," \
           "\n\t\t\t\"SQL\": \"" + sql_statement + "\"\n\t\t\t}"

def format_end_log(subject, full_msg, additional_fields):
    return "\n\t\t\t{\n\t\t\t\"Subject\": \"" + subject + "\"," \
           "\n\t\t\t\"Message\": \"" + full_msg + "\""\
           "\n\t\t\t\"Message\": \"" + additional_fields + "\"\n\t\t\t}"

def create_logger():
    file_name = str(sys.argv[0].split("/")[-1])


    state = file_name.split("_")[0]
    if not os.path.exists("logs"):
        os.makedirs("logs")
    if not os.path.exists("logs/" + state):
        os.makedirs("logs/" + state)
    log_loc = "logs/" + state + "/" + file_name + "_" + str(dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")) + ".log"
    # create logger
    logger = logging.getLogger("DDDB_Logger")
    logger.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("{\n\"Time\": \"%(asctime)s\","
                                  "\n\"File\": \"%(filename)s\","
                                  "\n\"Function\": \"%(funcName)s\","
                                  "\n\"Type\": \"%(levelname)s\","
                                  "\n\t\"Message\": %(message)s\n}")

    if len(sys.argv) == 1:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.warning)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_loc)
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


'''
A unified name formatting function.

Returns the cleaned name as a dictionary, with fields for the person's
first, middle, and last names, as well as any nicknames or suffixes

Inputs:
|name|: The person's full name, formatted as either 'first last' or 'last, first'
|problem_names|: Some names have inconsistencies across different sources.
                 This optional parameter should contain a dictionary mapping the incorrect names to the correct names
'''
def clean_name(name, problem_names={}):
    suffixes = ['Jr.', 'Sr.', 'III', 'II', 'IV', "P.h.D.", "O.D.", "M.D.", "PhD."]
    titles = ['Dr.', 'Rev.', 'Mr.', 'Mrs.', 'Officer', 'Chief', 'Sheriff', 'Cpt.']

    person = dict()

    name = name.strip("\n").strip().strip("\t")

    # If the name matches one of the provided problem names, replace it with the correct name
    for key in problem_names.keys():
        keynames = key.split(' ')
        for kname in keynames:
            if kname not in name:
                break
            name = problem_names[key]

    # If the person's name has a suffix, add it to the person dictionary and remove it from the name
    person['suffix'] = ''
    for suffix in suffixes:
        if suffix in name:
            person['suffix'] = suffix
            name = name.replace(suffix, '')

    person['title'] = ''
    for title in titles:
        if title in name:
            person['title'] = title
            name = name.replace(title, '')

    # For names formatted "First Last, Suffix", remove the trailing comma
    name = name.strip().strip(',')

    # Check if a person has a nickname in quotes, eg. Wengay "Newt" Newton
    person['nickname'] = ''
    if "\"" in name:
        nickname = re.findall(r'"(.*?)"', name)
    else:
        nickname = re.findall(r'\(([^)]*)\)', name)
    if len(nickname):
        person['nickname'] = nickname[0]
        name = name.replace("\"" + nickname[0] + "\"", "") \
                   .replace("(" + nickname[0] + ")","").strip()

    # Split the name on the comma for names formatted as "Last, First"
    split_name = [word.strip() for word in name.split(',')]

    # This branch is taken if the name was formatted "Last, First"
    if len(split_name) > 1:
        # The last name is the part of the name before the comma
        person['last'] = split_name[0]
        # Set the rest of the name as the first name for now
        person['first'] = ' '.join([word.strip() for word in split_name[1:]])

    # This branch gets taken if the name is formatted "First Last"
    else:
        space_split = name.split(' ')

        # Check if it's a name like Kevin De Leon; use De Leon as last if so
        if space_split[-2].lower() == 'de':
            person['last'] = ' '.join([word.strip() for word in space_split[-2:]])
            person['first'] = ' '.join([word.strip() for word in space_split[:-2]])
        # Otherwise, last name is the last word, first is everything else
        else:
            person['last'] = space_split[-1]
            person['first'] = ' '.join([word.strip() for word in space_split[:-1]])


    person['first'] = person['first'].strip()

    # Finally, check to see if the person has middle names
    given_names = person['first'].split(' ')
    person['middle'] = ''
    if len(given_names) > 1:
        person['first'] = given_names[0]
        person['middle'] = ' '.join([word.strip() for word in given_names[1:]])
    person['like_name'] = (person['first'] + "%" + person['last'] + " " + person['suffix']).strip()
    person['like_last_name'] = "%" + person['last']
    person['like_first_name'] = person['first'] + "%"
    person['like_nick_name'] = (person['nickname'] + "%" + person['last'] + " " + person['suffix']).strip()

    for field in person:
        if person[field] == '':
            person[field] = None
        else:
            person[field] = person[field].strip()
    return person
