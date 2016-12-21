from ImportGiftDataRefactored import *


CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2015Dec',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


def main():

    cnxn = pymysql.connect(**CONN_INFO)

    gift_df = pickle.load(open('gift_df.p', 'rb'))
    leg_lop_df = pickle.load(open('leg_lop_df.p', 'rb'))
    leg_lop_df = leg_lop_df.rename(columns = {'first': 'staff_first',
                                              'middle': 'staff_middle',
                                              'last': 'staff_last',
                                              'pid': 'staff_member',
                                              'leg_pid': 'legislator',
                                              'hire_date': 'start_date'
                                              })
    # leg_lop_df['end_date'] = leg_lop_df.apply(correct_end_dates, axis=1)
    # op_df = pickle.load(open('op_df.p', 'rb'))

    gift_df = clean_data(gift_df)

    d_gift_df = gift_df[~pd.isnull(gift_df.D_gift_value)].copy()
    e_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value)].copy()
    mix_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value) & ~pd.isnull(gift_df.D_gift_value)]
    assert mix_gift_df.shape[0] == 0


    d_gift_df = rename_columns(d_gift_df, 'D')
    d_gift_df['og_index'] = d_gift_df.index
    e_gift_df = rename_columns(e_gift_df, 'E')
    e_gift_df['og_index'] = e_gift_df.index

    d_gift_df = d_gift_df.sample(200)
    e_gift_df = e_gift_df.sample(200)


    leg_lop_df = leg_lop_df[pd.notnull(leg_lop_df.staff_member) &
                            pd.notnull(leg_lop_df.legislator)]
    d_sample = leg_lop_df[['staff_member', 'legislator']].sample(200)
    e_sample = leg_lop_df[['staff_member', 'legislator']].sample(200)

    d_gift_df['staff_member'] = d_sample['staff_member'].values
    d_gift_df['legislator'] = d_sample['legislator'].values
    e_gift_df['staff_member'] = e_sample['staff_member'].values
    e_gift_df['legislator'] = e_sample['legislator'].values

    d_gift_df['schedule'] = 'D'
    e_gift_df['schedule'] = 'E'

    cols = ['year_filed',
            'agency_name',
            'staff_member',
            'legislator',
            'position',
            'district_number',
            'jurisdiction',
            'source_name',
            'source_city',
            'source_state',
            'source_business',
            'cleaned_date',
            'gift_value',
            'gift_description',
            'image_url',
            'schedule']

    rename_dict = {'year_filed': 'year', 'cleaned_date': 'date_given'}
    d_gift_df = d_gift_df[cols].rename(columns=rename_dict)
    d_gift_df['speech_or_panel'] = None
    d_gift_df['reimbursed'] = None
    e_gift_df = e_gift_df[cols].rename(columns=rename_dict)
    e_gift_df['speech_or_panel'] = None
    e_gift_df['reimbursed'] = None

    d_gift_df.to_sql(con=cnxn, name='LegStaffGifts', flavor='mysql', if_exists='append', index=False)
    e_gift_df.to_sql(con=cnxn, name='LegStaffGifts', flavor='mysql', if_exists='append', index=False)





if __name__ == '__main__':
    main()
