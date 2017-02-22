import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.ticker as ticker
from matplotlib.colors import ListedColormap
from matplotlib import colors
import matplotlib.patches as mpatches
from matplotlib.legend_handler import HandlerPatch
import matplotlib.pyplot as plt
import sqlalchemy
import json
CONFIG_PATH = '/mnt/data/johnson_county/config/default_profile.json'
import seaborn as sns
sns.set(color_codes=True,font='monospace',font_scale=1.4)

with open(CONFIG_PATH) as f:
    config = json.load(f)
engine = sqlalchemy.create_engine('postgres://', connect_args=config)

class UserTimeline(object):
    def __init__(self, personid):
        self.connection = engine
        self.personid = personid
        self.user = self.get_data()

    def get_data(self):
        ''' Return a user DataFrame which contains columns of personid, begin_date and event

        :params int perosnid: The personid/hash_ssn that we want to draw the timeline of.
        :params connection: A sqlalchemy connection object to connect to the database.
        :return: A user DataFrame.
        :rtype: pandas.DataFrame
        '''

        user = pd.read_sql("SELECT hash_ssn as personid, begin_date, event FROM premodeling.hashssn_event_dates where hash_ssn = decode('{}', 'hex');".format(str(self.personid).encode('hex')), self.connection)
        user['event'] = user['event'].map({'ems':'E','mh':'M','booking':'J'})
        return user

    def get_series(self,freq='1M'):
        user_df = self.user.sort_values(by='begin_date')
        user_series = pd.Series(list(user_df['event']), index=user_df['begin_date'])
        user_series.index = pd.DatetimeIndex(user_series.index)
        user_resample = user_series.resample('1D').sum()
        time = pd.date_range(start='2010-01',end='2016-06',freq='1D')
        user_resample = user_resample.reindex(time,fill_value=0)
        user_resample = user_resample.astype('str')
        user_resample = user_resample.resample(freq).sum()
        for i in range(len(user_resample)):
            user_resample[i] = "".join(user_resample[i].split('0'))
        return user_resample

    def draw_timeline(self, tobesaved=False, fname=None, y_max=20, freq='1M'):
        ''' Prints a user's timeline of interaction with ems, jail,and mental health from 2010 Jan to 2016 June

        :params DataFrame user: A DataFrame of user with personid/hash_ssn, event, begin_date and end_date.
        :params str fname: The file name to be saved with.
        :params bool tobesaved: The flag of whether to save the figure.
        :params int y_max: The maximum of y-axis of the figure.
        :return: None
        :rtype: None
        '''
        user_resample = self.get_series(freq)
        user_m = [t.count('M') for t in user_resample]
        user_e = [t.count('E') for t in user_resample]
        user_j = [t.count('J') for t in user_resample]
        user_df = pd.DataFrame({'count_of_m':user_m, 'count_of_j':user_j, 'count_of_e':user_e},index=list(user_resample.index.to_period(freq)))
        columns = list(user_resample.index.to_period(freq))
        x_max = len(columns)
        heat_df = pd.DataFrame(np.array([[0]*x_max]*y_max), index=range(y_max), columns=columns)
        for i in range(len(user_df)):
            temp = []
            temp_df = user_df.iloc[[i]]
            if int(temp_df['count_of_e']) > 0:
                temp.extend([1]*int(temp_df['count_of_e']))

            if int(temp_df['count_of_j']) > 0:
                temp.extend([2]*int(temp_df['count_of_j']))

            if int(temp_df['count_of_m']) > 0:
                temp.extend([3]*int(temp_df['count_of_m']))

            temp = temp + [0]*(y_max-len(temp))
            heat_df[columns[i]] = temp

        fig = plt.figure(figsize=(30,10))
        #plt.title("{}'s timeline".format(user['personid'][0]))
        cmap = ListedColormap(['white',(0.99,0.58,0.59),(0.59,0.59,1),(0.6,0.8,0.59)])
        ax = sns.heatmap(heat_df, cmap=cmap,vmin=0,vmax=3,edgecolor='w', linewidth=1.5,cbar=False, annot=False, square=True)
        ax.invert_yaxis()
        ax.set_xticklabels(columns,rotation=90,ha='center',va='top')
        ax.set_yticklabels(range(y_max,0,-1), rotation=0,va='baseline')

        ems_patch = mpatches.Patch(color=(0.99,0.58,0.59), label='EMS')
        jail_patch = mpatches.Patch(color=(0.59,0.59,1), label='Jail')
        mh_patch = mpatches.Patch(color = (0.6,0.8,0.59), label='Mental Health')
        plt.legend(handles=[ems_patch,jail_patch,mh_patch],
                            handler_map={jail_patch: HandlerSquare(), ems_patch: HandlerSquare(), mh_patch: HandlerSquare()},
                            bbox_to_anchor=(0.5, 1.05), loc=9, ncol=3, borderaxespad=0.3,prop={'size':25})

        if tobesaved == True:
            if fname == None:
                fname = "id{}_timeline.png".format(user['personid'][0])
            plt.savefig(fname)

        plt.show()

class HandlerSquare(HandlerPatch):
    ''' A HandlerSquare class inherited from HandlerPatch class which is a legend handler object of matplotlib
    '''
    def create_artists(self, legend, orig_handle, xdescent, ydescent, width, height, fontsize, trans):
        ''' Return square patch legend list which is going to used in legend handles

        :params : all the arguments to make a square legend
        :return: A list of mpatches object
        :rtype: list
        '''
        center = xdescent + 0.5 * (width - height), ydescent
        p = mpatches.Rectangle(xy=center, width=height,height=height, angle=0.0)
        self.update_prop(p, orig_handle, legend)
        p.set_transform(trans)
        return [p]
