#import math
from datetime import datetime
import typing
import numpy as np
#from numpy.core.fromnumeric import mean
import pandas as pd
#from pandas.core.indexes.api import get_objs_combined_axis
# import random
# import itertools
#import sys
##############
#class prep(object):
class prep():
    def __init__(self,
    df: pd.core.frame.DataFrame,
                   date_variable: typing.Union[int, str],
                   value_variable: typing.Union[int, str],
                   field_list: list,
                   fill_missing:bool = True,
                   cycle_length: int = 12
                   ):
        self.date_variable = date_variable
        self.cycle_length = cycle_length
        # added step to remove any duplicates
        self.df = df.drop_duplicates()
        self.fill_missing = fill_missing
        #self.list_field = field_list

        self.list_field = field_list.copy()

        self.value_variable = value_variable
        self.list_df = df[self.list_field].drop_duplicates().reset_index().copy()
        #  Ensure date variable is correct datetime format
        self.df[self.date_variable] = pd.to_datetime(self.df[self.date_variable])       
        # used to check every iteration for completeness of observations, otherwise fill with 0
        range = pd.date_range(start=df[self.date_variable].min(),end=df[self.date_variable].max(),freq='MS').to_series().sort_values()
        self.range = pd.DataFrame(range)
        self.range.columns = [self.date_variable]

        self.df_ready,self.list_df_srl = self.serialize()
        
        self.df_ready = self.fill_blanks()
        
        self.df_ready.drop('index',axis=1,inplace=True)
  
    def serialize(self):
        """
        This function takes in a data frame with multiple dimensions and serializes the data frame, to iterate over each combination that requires a forecast.
        """      
        list_df = self.list_df.copy()
        df = self.df.copy()

        list_df['srl_num'] = pd.Series(range(0,list_df.shape[0]))
        df_ready = self.df.merge(list_df.drop(['index'],axis=1),how='inner',on=self.list_field)
        #print('at serialize stage',df_ready.dtypes)
        df_ready.srl_num = df_ready.srl_num.astype('str') 

        self.list_field.extend([self.value_variable,self.date_variable,'srl_num'])
        #keep only f_list items, date, and value variables
        df_ready = df_ready[self.list_field].drop_duplicates().reset_index().copy()
        self.list_field.remove(self.value_variable)
        df_ready = df_ready.groupby(self.list_field).sum().reset_index()
        return df_ready, list_df

    def fill_blanks(self):
        stage_df = self.df_ready.copy()
        for comb in stage_df.srl_num.unique(): 
            temp = stage_df[stage_df['srl_num'] == comb].copy()
            if self.fill_missing:
                if (temp.shape[0] < self.range.shape[0]) & (temp.shape[0] >= 2* self.cycle_length):
                    # remove incomplete subset of data from self.df_ready
                    stage_df = stage_df[stage_df['srl_num'] != comb]

                    temp2 = self.range.merge(temp,how='left',on=self.date_variable)
                    # since we are filling missing data, other fields will be missing to
                    # value info will be filled by zero, for other fields, we simply forward fill
                    # Then back fill for full missing value coverage    
                    temp2[self.value_variable].fillna(0,inplace=True)
                    temp2.fillna(method='ffill',inplace=True)
                    temp2.fillna(method='bfill',inplace=True)
                    # replace with new, full data subset
                    stage_df = stage_df.append(temp2,ignore_index=True)
            if temp.shape[0] < 2 * self.cycle_length:
                print(comb,'Does not contain a minimum of ',2*self.cycle_length,' observations')
                print(list(temp.iloc[0,:-4]))
                # remove from series
                stage_df = stage_df[stage_df.srl_num != comb]
        #print('at fill blanks stage',stage_df.dtypes)
        return stage_df