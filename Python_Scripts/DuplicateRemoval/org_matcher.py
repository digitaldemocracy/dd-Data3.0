#### NOTE: NEEDS TO BE RUN WITH 2.7 ####


import pandas as pd

# This is all black magic to me, but I'm pretty sure it prevents multiple instantiations of an OrgMatcher class
class SingletonType(type):
    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super(SingletonType, cls).__call__(*args, **kwargs)
            return cls.__instance

# Given the base name in the database, builds a series of names that can be matched against
# Returns: A pandas series with the oid and a several names to be matched against
def build_names(row):

    out = {}
    normed_name = unidecode(row['name']).lower().strip()


# Class is designed to handle the matching of org names to existing ones in our database. Object is
# designed to be a singleton
class OrgMatcher(object):

    __metaclass__ = SingletonType

    def __init__(self, cursor):
        query = """SELECT oid, name
                   FROM Organizations"""
        cursor.execute(query)

        org_lists = [[oid, name] for oid, name in cursor]
        org_df = pd.DataFrame(org_lists, columns=['oid', 'name'])



    def test_method(self):
        print("x", self.x, "y", self.y, "z")


