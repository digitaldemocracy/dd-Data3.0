import pandas as pd
import datetime

# If the term is greater than the end date year, reassign the ned date to the beginning of the next
# term year
# Returns: The corrected end_date
def correct_end_dates(leg_op_row):
    term_year = leg_op_row['term_year']
    end_date = leg_op_row['end_date']
    if term_year != 2015:
        if pd.isnull(end_date) or end_date.year > term_year:
            end_year = term_year + 1
            end_date = datetime.date(end_year, 12, 31)
    return end_date



