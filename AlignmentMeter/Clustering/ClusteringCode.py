import pandas as pd
import numpy as np
import pymysql

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib.patches as mpatches

CONN_INFO = {'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


def get_data(cnxn):
    q = """SELECT p.first, p.last, o.name, s.*, t.district
           FROM Person p
               JOIN LegAlignmentScores s
                   ON p.pid = s.pid
               JOIN OrgConcept o
                   on s.oid = o.oid
                JOIN Term t
                    ON t.pid = s.pid
                        AND t.year = s.session_year
           WHERE s.no_unanimous = 1
               and s.no_resolutions = 1
               and s.no_abstain_votes = 1
               and session_year = '2015'
        """
    data = pd.read_sql(q, cnxn)

    return data

def make_leg_scores(data):
    df_lst = []
    for g, g_df in data.groupby(['pid']):
        df = g_df[['oid', 'score']].transpose()
        df.columns = df.loc['oid']
        df.drop('oid', inplace=True)
        df.index = [g]
        df_lst.append(df)

    return pd.concat(df_lst)


def get_leg_info(data):
    return data[['pid', 'first', 'last', 'district', 'party', 'house']].drop_duplicates()

def get_org_info(data):
    return data[['oid', 'name']].drop_duplicates()


def group_metrics(g_df, org_info_df):

    oids = org_info_df.oid.unique()

    dems = sum(g_df.party == 'Democrat')
    reps = sum(g_df.party == 'Republican')
    sen = sum(g_df.house == 'Senate')
    asm = sum(g_df.house == 'Assembly')

    avgs = g_df.loc[:, oids].mean()
    avgs = avgs.to_frame('avg_score').reset_index()
    cols = ['name', 'avg_score']
    avgs = avgs.merge(org_info_df, on='oid')[cols]

    avgs = avgs.transpose()
    avgs.columns = avgs.loc['name']
    avgs.drop('name', inplace=True)

    d = pd.Series({'dems': dems, 'reps': reps, 'senate': sen, 'assembly': asm})

    return pd.concat([avgs.iloc[0], d])

def make_base_map(house):
    m = Basemap(resolution='h',  # c, l, i, h, f or None
                projection='merc',
                lat_0=37.205, lon_0=-119.525,
                llcrnrlon=-125.05, llcrnrlat=32.32,
                urcrnrlon=-114, urcrnrlat=42.09)

    if house == 'Senate':
        m.readshapefile('ShapeData/Senate/cb_2015_06_sldu_500k', 'districts')
    elif house == 'Assembly':
        m.readshapefile('ShapeData/Assembly/cb_2015_06_sldl_500k', 'districts')
    else:
        assert False

    return m


def create_poly_df(data, m, house):
    districts = []
    clusters = []
    for dist in m.districts_info:
        dist_num = int(dist['NAME'])
        cluster = data[(data.district == dist_num) &
                       (data.house == house)].iloc[0]['cluster_name']
        districts.append(dist['NAME'])
        clusters.append(cluster)
    shapes = [Polygon(np.array(shape), True) for shape in m.districts]
    df_poly = pd.DataFrame({'shapes': shapes,
                            'district': districts,
                            'cluster': clusters})
    return df_poly


def plot_map(df_poly, m):
    fig, ax = plt.subplots(figsize=(10, 20))

    m.drawmapboundary(fill_color='#46bcec')
    m.fillcontinents(color='#f2f2f2', lake_color='#46bcec')
    # m.fillcontinents(color='#ddaa66',lake_color='aqua')

    m.drawcoastlines()

    pc = PatchCollection(df_poly[df_poly.cluster == 'liberal']['shapes'],
                         facecolor='navy',
                         linewidths=1.,
                         zorder=2)
    ax.add_collection(pc)

    pc = PatchCollection(df_poly[df_poly.cluster == 'moderate conservative']['shapes'],
                         facecolor='firebrick',
                         linewidths=1.,
                         zorder=2)
    ax.add_collection(pc)

    pc = PatchCollection(df_poly[df_poly.cluster == 'conservative']['shapes'],
                         facecolor='maroon',
                         linewidths=1.,
                         zorder=2)
    ax.add_collection(pc)

    pc = PatchCollection(df_poly[df_poly.cluster == 'moderate liberal']['shapes'],
                         facecolor='royalblue',
                         linewidths=1.,
                         zorder=2)
    ax.add_collection(pc)

    ld_patch = mpatches.Patch(color='navy', label='Liberal')
    sd_patch = mpatches.Patch(color='royalblue', label='Moderate Liberal')
    lr_patch = mpatches.Patch(color='maroon', label='Conservative')
    sr_patch = mpatches.Patch(color='firebrick', label='Moderate Conservative')

    plt.legend(handles=[ld_patch, sd_patch, lr_patch, sr_patch])