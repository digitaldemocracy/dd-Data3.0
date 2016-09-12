from ImportGiftDataRefactored import *

def main():
    with pymysql.connect(**CONN_INFO) as cursor:
        gift_df = pickle.load(open('gift_df.p', 'rb'))
        leg_terms_df = pickle.load(open('leg_terms_df.p', 'rb'))
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
        leg_lop_df = add_districts(cursor, leg_lop_df)

        gift_df = clean_data(gift_df)

        d_gift_df = gift_df[~pd.isnull(gift_df.D_gift_value)].copy()
        e_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value)].copy()
        mix_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value) & ~pd.isnull(gift_df.D_gift_value)]
        assert mix_gift_df.shape[0] == 0

        d_gift_df = rename_columns(d_gift_df, 'D')
        d_gift_df['og_index'] = d_gift_df.index
        e_gift_df = rename_columns(e_gift_df, 'E')
        e_gift_df['og_index'] = e_gift_df.index
        # It's important that you don't overwrite any data
        assert len(d_gift_df.columns & leg_lop_df.columns & APPENDED_INFO) == 0
        assert len(e_gift_df.columns & leg_lop_df.columns & APPENDED_INFO) == 0

        # e_gift_df = e_gift_df.sample(10)
        pickle.dump(e_gift_df, open('e_gift_df.p', 'wb'))
        pickle.dump(d_gift_df, open('d_gift_df.p', 'wb'))
        leg_idx = (e_gift_df['person_type'] == 'Assemblymember') | (e_gift_df['person_type'] == 'Senator')
        # e_gift_df_legs = e_gift_df.loc[leg_idx].apply(lambda row: match_leg(row, leg_terms_df, set(leg_lop_df.columns)),
        #                                     axis=1)
        # pickle.dump(e_gift_df_legs, open('e_gift_df_legs.p', 'wb'))

        dir = 'FinalOutput'
        match_file = 'matched_sched_d_2011'
        unmatch_file = 'unmatched_sched_d_2011'
        match_file = dir + '/' + match_file
        unmatch_file = dir + '/' + unmatch_file
        years = [2011]

        my_df = d_gift_df
        my_columns = COLUMNS_ORDER_D
        staff_idx = my_df['person_type'] == 'Staff'
        my_df = my_df[staff_idx]
        my_df = my_df[my_df.year_filed.isin(years)]

        # my_df = my_df.sample(10)

        my_df = my_df.apply(lambda row: match_staff_member(row, leg_lop_df), axis=1)
        matched = my_df[(my_df['vanilla_match']) |
                        (my_df['multi_match'])]
        unmatched = my_df[~(my_df['vanilla_match']) &
                          ~(my_df['multi_match'])]

        pickle.dump(matched, open(match_file + '.p', 'wb'))
        pickle.dump(unmatched, open(unmatch_file + '.p', 'wb'))
        matched.to_excel(match_file + '.xlsx', columns=my_columns)
        unmatched.to_excel(unmatch_file + '.xlsx', columns=my_columns)



if __name__ == '__main__':
    main()