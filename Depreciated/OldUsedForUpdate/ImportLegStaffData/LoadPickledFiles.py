import pickle
import openpyxl as xl

def write_saved_rows(saved_rows):
    wb = xl.Workbook()
    ws = wb.active
    order = ['match_staff_flag',
             # 'match_leg_flag',
             'year',
             'agency_name',
             'matched_leg_last',
             'matched_leg_first',
             'matched_leg_middle',
             'last_name',
             'first_name',
             'matched_staff_last',
             'matched_staff_first',
             'person_type',
             'position',
             'district_number',
             'jurisdiction',
             'source_name',
             'source_city',
             'source_state',
             'source_business',
             'date_given',
             'original_date',
             'gift_value',
             'reimbursed',
             'gift_description',
             'E_source_name',
             'E_source_city',
             'E_source_state',
             'E_source_business',
             'E_date_given',
             'E_gift_value',
             'gift_or_income',
             'speech_or_panel',
             'E_gift_description',
             'image_url',
             'legislator',
             'staff_member'
             ]

    # f.write(','.join(order))
    for row in saved_rows:
        # f.write('\n')
        row_vals = [str(row[k]) for k in order]
        assert len(order) == len(row_vals)
        ws.append(row_vals)
        # f.write(','.join(row_vals))

    return wb


# missed_staff = pickle.load(open('MissedStaff.p', 'r'))
# with open('MissedStaff.csv', 'w') as out_f:
#     for staff in missed_staff:
#         out_f.write(str(staff))

# saved_rows = pickle.load(open('SavedRowsNew.p', 'rb'))
# # with open('AppendedGiftData2.csv', 'w') as f:
# wb = write_saved_rows(saved_rows)
# wb.save('AppenededGiftData.xlsx')
