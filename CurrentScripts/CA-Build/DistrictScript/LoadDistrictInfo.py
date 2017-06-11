'''
Grabs demographic info from statewidedatabase.org. This script is just thrown together as it should
really only be run once.
'''



from requests import get
from io import BytesIO
from zipfile import ZipFile
import pysal
import pandas as pd
import pymysql


zip_url = 'http://statewidedatabase.org/pub/data/G12/state/state_g12_sov_data_by_g12_svprec.zip'
CONN_INFO = {'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


def load_data():
    # Load and extract the zip file
    request = get(zip_url)
    zip_file = ZipFile(BytesIO(request.content))
    zip_file.extractall()

    # Loads the data into a dataframe
    files = zip_file.namelist()
    dbf_name = [f for f in files if '.dbf' in f.lower()][0]
    dbf = pysal.open(dbf_name, 'r')
    df = dbf.to_df()

    new_cols = {'ADDIST': 'asm_dist',
                'SDDIST': 'sen_dist',
                'PRSDEM01': 'democrats',
                'PRSREP01': 'republicans'}
    df = df[list(new_cols.keys())].rename(columns=new_cols)

    # The zero districts are aggregated info, we want to exclude this
    return df[(df.asm_dist != 0) & (df.sen_dist != 0)]

def aggregate_dist_info(df):
    sen_df = df.groupby('sen_dist')[['republicans', 'democrats']].sum()
    sen_df = sen_df.reset_index().rename(columns={'sen_dist': 'district'})
    sen_df['house'] = 'Senate'

    asm_df = df.groupby('asm_dist')[['republicans', 'democrats']].sum()
    asm_df = asm_df.reset_index().rename(columns={'asm_dist': 'district'})
    asm_df['house'] = 'Assembly'

    df = pd.concat([sen_df, asm_df])
    df['state'] = 'CA'
    df['year'] = 2012
    df['district'] = df.district.astype(int)

    return df

def create_table(cursor):
    q = """DROP TABLE IF EXISTS DistrictInfo"""
    cursor.execute(q)

    q = """CREATE TABLE DistrictInfo (
      district INT,
      house VARCHAR(200),
      republicans INT,
      democrats INT,
      year INT,
      state VARCHAR(2),

      PRIMARY KEY (district, house, state, year)
        )"""
    cursor.execute(q)

def main():

    df = load_data()
    df = aggregate_dist_info(df)

    cnxn = pymysql.connect(**CONN_INFO)
    cursor = cnxn.cursor()

    create_table(cursor)

    df.to_sql('DistrictInfo', con=cnxn, flavor='mysql', if_exists='append', index=False)

    cursor.close()


if __name__ == '__main__':
    main()
