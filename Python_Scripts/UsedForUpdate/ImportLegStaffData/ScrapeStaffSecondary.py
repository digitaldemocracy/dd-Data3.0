import os, os.path
import pymysql
import pickle
import pandas as pd

from ScrapeStaffBestRefactored import get_staff_pid

CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'AndrewTest',
             # 'db': DDDB2015Dec',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


# Inserts a new leg office personnel into the database and updates your version
# Returns: Nothing, updates leg_lop_df
def insert_new_lop_secondary(cursor, staff_info, leg_lop_df):

    insert_stmt = '''INSERT INTO LegOfficePersonnel
                     (staff_member, legislator, term_year, house, start_date, end_date, state)
                     VALUES
                     (%s, %s, %s, %s, %s, %s, "CA")'''
    # Note that 15 gets auto-converted to 2015 by mysql
    cursor.execute(insert_stmt, (int(staff_info['pid']), int(staff_info['leg_pid']), staff_info['term_year'],
                                 staff_info['house'], str(staff_info['hire_date']), str(staff_info['end_date'])))

    rows = leg_lop_df.shape[0]
    leg_lop_df.loc[len(leg_lop_df.index)] = staff_info
    assert leg_lop_df.shape[0] == rows + 1, 'Failed to add a row'


def main():
    with pymysql.connect(**CONN_INFO) as cursor:

        db_leg_term_infos = pickle.load(open('SavedLegTermInfo.p', 'rb'))
        db_leg_term_df = pd.DataFrame(db_leg_term_infos)
        leg_lop_df = pickle.load(open('leg_lop_df.p', 'rb'))
        leg_lop_df['secondary_source'] = False
        existing_staff_df = pickle.load(open('existing_staff_df.p', 'rb'))

        existing_count = 0
        existing_for_staff_count = 0
        new_count = 0
        for idx, leg_term_row in db_leg_term_df.iterrows():

            matched_terms_df = leg_lop_df[(leg_lop_df.leg_pid == leg_term_row['pid']) &
                                          (leg_lop_df.term_year == leg_term_row['term_year']) &
                                          (leg_lop_df.house == leg_term_row['house'])]
            assert len(matched_terms_df.index) > 0

            for first, middle, last in leg_term_row['staff_set']:
                add_staff = False
                date_dict = leg_term_row['staff_set'][(first, middle, last)]
                start_date = date_dict['start_date']
                end_date = date_dict['end_date']

                staff_pid = get_staff_pid(cursor, first, middle, last, existing_staff_df)

                matched_personnel_df = matched_terms_df[matched_terms_df.pid == staff_pid]

                assert len(matched_personnel_df) < 2, 'Wait, so they had two stints with the same legislator over the ' \
                                                  'same term?'

                # This block is just for accounting purposes
                if staff_pid in leg_lop_df['pid']:
                    existing_count += 1
                    if staff_pid in matched_terms_df['pid']:
                        existing_for_staff_count += 1
                    else:
                        pass
                else:
                    new_count += 1
                    assert len(matched_personnel_df.index) == 0
                    add_staff = True

                if add_staff:
                    staff_info = {k: None for k in leg_lop_df.columns}
                    staff_info['secondary'] = True
                    staff_info['pid'] = staff_pid
                    staff_info['leg_pid'] = leg_term_row['pid']
                    staff_info['term_year'] = leg_term_row['term_year']
                    staff_info['house'] = leg_term_row['house']
                    staff_info['hire_date'] = start_date
                    staff_info['end_date'] = end_date

                    insert_new_lop_secondary(cursor, staff_info, leg_lop_df)

        pickle.dump(leg_lop_df, open('leg_lop_df.p', 'wb'))
        pickle.dump(existing_staff_df, open('existing_staff_df.p', 'wb'))










if __name__ == '__main__':
    main()






