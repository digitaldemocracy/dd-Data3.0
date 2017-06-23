import re


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
    suffixes = ['Jr.', 'Sr.', 'III', 'II', 'IV']

    person = dict()

    name = name.strip()

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

    # For names formatted "First Last, Suffix", remove the trailing comma
    name = name.strip().strip(',')

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

    # Check if a person has a nickname in quotes, eg. Wengay "Newt" Newton
    person['nickname'] = ''
    nickname = re.search(r'".*?"', person['first'])
    if nickname is not None:
        person['nickname'] = nickname.group(0)
        person['first'] = person['first'].replace(nickname.group(0), '')

    person['first'] = person['first'].strip()

    # Finally, check to see if the person has middle names
    given_names = person['first'].split(' ')
    person['middle'] = ''
    if len(given_names) > 1:
        person['first'] = given_names[0]
        person['middle'] = ''.join([word.strip() for word in given_names[1:]])

    return person


