# Selects
QS_DISTRICT = '''SELECT *
                FROM District
                WHERE did=%(did)s
                AND house=%(house)s
                AND state=%(state)s'''

# Inserts
QI_DISTRICT = '''INSERT INTO District
                (state, house, did, note, year, region, geoData)
                VALUES
                (%(state)s, %(house)s, %(did)s, %(note)s, %(year)s, %(region)s, %(geoData)s)'''