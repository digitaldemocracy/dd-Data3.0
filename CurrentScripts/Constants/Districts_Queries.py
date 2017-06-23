# Selects
QS_DISTRICT = '''SELECT *
                FROM District
                WHERE did=%s
                AND house=%s
                AND state=%s'''

# Inserts
QI_DISTRICT = '''INSERT INTO District
                (state, house, did, note, year, region, geoData)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s)'''