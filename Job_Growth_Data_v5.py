import pandas as pd
import os
import time
from datetime import datetime
from urllib2 import urlopen, URLError, HTTPError
from sqlalchemy import create_engine
import subprocess
import urllib
import MySQLdb
import re


############################################################################################################################
# The purpose of this document...
#
#
#
#
#
#

engine = create_engine("mysql+mysqldb://job_growthmaster:jobgrowthpass@job-growth.cotzely14ram.us-west-2.rds.amazonaws.com/job_growth2?charset=utf8&use_unicode=0")




files = ['sm/sm.data.1.AllData',
         'ce/ce.data.0.AllCESSeries',
         'ce/ce.data.07.TotMinConAECurr',
         'ce/ce.data.08.ManufactureAECurr',
         'ce/ce.data.09.ServProvTradeAECurr',
         'ce/ce.data.10.TransWhUtsAECurr',
         'ce/ce.data.11.InfoAECurr',
         'ce/ce.data.12.FinActAECurr',
         'ce/ce.data.13.ProfBusAECurr',
         'ce/ce.data.14.EducHealthAECurr',
         'ce/ce.data.15.LeisHospAECurr',
         'ce/ce.data.16.OtherServicesAECurr',
         'ce/ce.data.17.GovtAECurr']

ce_files = ['ce/ce.data.07.TotMinConAECurr',
          'ce/ce.data.08.ManufactureAECurr',
          'ce/ce.data.09.ServProvTradeAECurr',
          'ce/ce.data.10.TransWhUtsAECurr',
          'ce/ce.data.11.InfoAECurr',
          'ce/ce.data.12.FinActAECurr',
          'ce/ce.data.13.ProfBusAECurr',
          'ce/ce.data.14.EducHealthAECurr',
          'ce/ce.data.15.LeisHospAECurr',
          'ce/ce.data.16.OtherServicesAECurr',
          'ce/ce.data.17.GovtAECurr']


dir = os.path.abspath(os.path.dirname(__file__))
datadir = dir + "\\data"
os.chdir(datadir)

data_hostname = "http://download.bls.gov/pub/time.series/"
current_filesystem = datadir



############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#



for filename in files: # Loop through the files in files dictonary
    filename_extension = filename[3:] + ".txt" # Filename munge
    data_location = data_hostname + "" + filename # file name location
    full_filepath = current_filesystem + "/" + filename_extension # full location
    print "downloading from: " + data_location
    urllib.urlretrieve(data_location, full_filepath) # grab that shit
    print "download path: " + full_filepath

print "Finished Downloading Data"

def strip(text):
    try:
        return text.strip()
    except AttributeError():
        return text


#curs.execute("SELECT month FROM date_ref_t")
#date = curs.fetchone()
#year_to_date = date[0]
year_to_date = 5


############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#

try:

    sm_state = pd.read_sql_query('SELECT * FROM sm_state', engine)
    sm_state['state_name'].replace('\\r','')

    sm_supersector = pd.read_sql_query('SELECT * FROM sm_supersector', engine)
    sm_supersector['supersector_code'] = sm_supersector['supersector_code'].astype(str)
    sm_supersector['supersector_code'] = sm_supersector['supersector_code'].apply( lambda x: x.zfill(8))
    sm_supersector['supersector_name'] = sm_supersector['supersector_name'].replace('\\r','')


    sm_series = pd.read_sql_query('SELECT * FROM sm_series', engine)
    sm_series['area_code'] = sm_series['area_code'].astype(str)
    sm_series['area_code'] = sm_series['area_code'].apply(lambda x: x.zfill(5))
    sm_series['industry_code'] = sm_series['industry_code'].astype(str)
    sm_series['industry_code'] = sm_series['industry_code'].apply(lambda x: x.zfill(8))
    sm_series['supersector_code'] = sm_series['supersector_code'].astype(str)
    sm_series['supersector_code'] = sm_series['supersector_code'].apply(lambda x: x.zfill(8))


    sm_industry = pd.read_sql_query('SELECT * FROM sm_industry', engine)
    sm_industry['industry_code'] = sm_industry['industry_code'].astype(str)
    sm_industry['industry_code'] = sm_industry['industry_code'].apply(lambda x: x.zfill(8))


    sm_datatype = pd.read_sql_query('SELECT * FROM sm_data_type', engine)

    sm_area = pd.read_sql_query('SELECT * FROM sm_area', engine)
    sm_area['area_code'] = sm_area['area_code'].str.replace('\n','')



    #sm_table = pd.read_sql_query('SELECT * FROM sm_data_1_AllData', engine)
    sm_table = pd.read_table("sm.data.1.AllData.txt",
                             converters = {'series_id' : strip,
                                        'year' : strip,
                                        'value' : strip,
                                        'footnote_codes': strip},
                              dtype = {'series_id' : object,
                                        'year' : object})

    sm_table_v1 = pd.merge(sm_table, sm_series, on = 'series_id')
#    del sm_table
    sm_table_v2 = pd.merge(sm_table_v1, sm_industry, on = 'industry_code')
#    del sm_table_v1
    sm_table_v3 = pd.merge(sm_table_v2, sm_supersector, on = 'supersector_code')
#    del sm_table_v2
    sm_table_v4 = pd.merge(sm_table_v3, sm_state, on = 'state_code')
#    del sm_table_v3
    sm_table_v5 = pd.merge(sm_table_v4, sm_area, on = 'area_code')
#    del sm_table_v4


    print "sm_table_v5 *************************************";
    # Rename columns

    sm_table_v5.rename(columns={'state_name_x':'state_name'}, inplace=True)
    #print sm_table_v5;

    sm_table_final = sm_table_v5[['area_name','state_name','supersector_name','industry_name','period','year','value','data_type_code','seasonal']]
#    del sm_table_v5

    print("sm_table_final is completed")

    sm_table_final['value'] = sm_table_final['value'].convert_objects(convert_numeric = True)
    sm_table_final['period'] = sm_table_final['period'].astype(str)


    ################################################################################################
    # Deal with MSA peculiarities
    sm_table_final = sm_table_final[(~sm_table_final.area_name.str.contains('Metropolitan Division'))]
    sm_table_final = sm_table_final[(~sm_table_final.area_name.str.contains('NECTA Division'))]

    sm_table_final['area_name'] = sm_table_final['area_name'].str.replace(' NECTA','')
    sm_table_final['area_name'] = sm_table_final['area_name'].str.replace('Metropolitan Statistical Area','')

    sm_table_final['state_name'] = sm_table_final['state_name'].str.replace('\r','')
    sm_table_final['supersector_name'] = sm_table_final['supersector_name'].str.replace('\r','')
    sm_table_final['data_type_code'] = sm_table_final['data_type_code'].astype(str)
    sm_table_final['data_type_code'] = sm_table_final['data_type_code'].apply(lambda x: x.zfill(2))


    sm_table_final_v2 = sm_table_final.query('seasonal == "U" and data_type_code == "01" and\
                                    state_name != "District of Columbia" \
                                    and state_name != "Virgin Islands"\
                                    and state_name != "Puerto Rico"\
                                    and period != "M13"')

    sm_table_final_v3 = sm_table_final.query('seasonal == "S" and data_type_code == "01" and\
                                    state_name != "District of Columbia" \
                                    and state_name != "Virgin Islands"\
                                    and state_name != "Puerto Rico"\
                                    and period != "M13"')

    sm_table_final_v3.rename(columns = {'period':'Month'}, inplace = True)
    sm_table_final_v3['Month'] = sm_table_final_v3['Month'].apply(lambda x: re.sub('M', '',x))

    sm_table_final_v2.rename(columns = {'period':'Month'}, inplace = True)
    sm_table_final_v2['Month'] = sm_table_final_v2['Month'].apply(lambda x: re.sub('M', '',x))


############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#

    #ce files
    ce_supersector = pd.read_sql_query('SELECT * FROM ce_supersector', engine)
    ce_supersector['supersector_code'] = ce_supersector['supersector_code'].astype(str)
    ce_supersector['supersector_code'] = ce_supersector['supersector_code'].apply(lambda x: x.zfill(8))


    ce_datatype = pd.read_sql_query('SELECT * FROM ce_data_type', engine)


    ce_industry = pd.read_sql_query('SELECT * FROM ce_industry', engine)
    ce_industry['industry_code'] = ce_industry['industry_code'].astype(str)
    ce_industry['industry_code'] = ce_industry['industry_code'].apply(lambda x: x.zfill(8))


    ce_period = pd.read_sql_query('SELECT * FROM ce_period', engine)


    ce_series = pd.read_sql_query('SELECT * FROM ce_series', engine)
    ce_series['series_id'] = ce_series['series_id'].astype(str)
    ce_series['series_id'] = ce_series['series_id'].apply(strip)
    ce_series['supersector_code'] = ce_series['supersector_code'].astype(str)
    ce_series['supersector_code'] = ce_series['supersector_code'].apply(lambda x: x.zfill(8))
    ce_series['industry_code'] = ce_series['industry_code'].astype(str)
    ce_series['industry_code'] = ce_series['industry_code'].apply(lambda x: x.zfill(8))


    ce_table_final = pd.DataFrame()

    # Creation of CE table from all of the individual files
    ce_table = pd.DataFrame()

    for table in ce_files:
      filename_extension = table[3:] + ".txt" # Filename munge
      print filename_extension
      temp_table = pd.read_table(filename_extension,
                                converters = {'series_id' : strip,
                                              'year'     : strip,
                                              'footnote_codes' : strip},
                                dtype = {'series_id' : object,
                                         'year'      : object})

      ce_table = ce_table.append(temp_table)

    print "Finished ce_table"

    ce_table_v1 = pd.merge(ce_table, ce_series, on = 'series_id')
    del ce_table
    ce_table_v2 = pd.merge(ce_table_v1, ce_industry, on = 'industry_code')
    del ce_table_v1
    ce_table_v3 = pd.merge(ce_table_v2, ce_supersector, on = 'supersector_code')
    del ce_table_v2

    ce_table_final = ce_table_final.append(ce_table_v3, ignore_index = True)
    ce_table_final = ce_table_final[['supersector_name','industry_name','period','year','value','data_type_code','seasonal']]
    del ce_table_v3

        # # The following code produces the tables with rankings, job growth, pct changes, and formats them to go
        # # into the db. They are turned into .csv's and then "database_load.R" just opens them and sends them to db
        # # my current local python build is have a hard time with ODBC drivers.

        # print("Running Data Munging")


        # # <codecell>

        # ce_table_final['value'] = ce_table_final['value'].convert_objects(convert_numeric = True)

        # ce_table_final['period'] = ce_table_final['period'].astype(str)

        # temp = sm_table_final
        # temp['area_name_split'] = temp.area_name.str.split(', ')
        # vector1 = temp.query('area_name == "Statewide"')['area_name_split']
        # vector2 = temp.query('area_name != "Statewide"')['area_name_split']
        # statewide = vector1.apply(lambda x: x[0])

        # msas   = vector2.apply(lambda x: x[0])
        # states = vector2.apply(lambda x: x[1])

        # msa_vector = statewide.append(msas)
        # states_vector = statewide.append(states)




    ################################################################################################
    ce_table_final_v2 = ce_table_final.query('seasonal == "U" and period != "M13"')
    ce_table_final_v3 = ce_table_final.query('seasonal == "S" and period != "M13"')



    ce_table_final_v3.rename(columns = {'period':'Month'}, inplace = True)
    ce_table_final_v3['Month'] = ce_table_final_v3['Month'].apply(lambda x: re.sub('M', '',x))

    ce_table_final_v2.rename(columns = {'period':'Month'}, inplace = True)
    ce_table_final_v2['Month'] = ce_table_final_v2['Month'].apply(lambda x: re.sub('M', '',x))

    print("************************  Created Final Tables: SM & CE")

    # ################################################################################################
    # # National percentage changes

############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#

    group_nat = ce_table_final_v3.groupby(['supersector_name','industry_name','year','Month'])
    sums_by_period = group_nat['value'].sum()
    sum_nat_mom = pd.DataFrame(sums_by_period).reset_index()
    sum_nat_mom.columns = ['supersector_name','industry_name','year','Month','value_mom']


    group_nat = ce_table_final_v2.groupby(['supersector_name','industry_name','year','Month'])
    sums_by_period = group_nat['value'].sum()
    sum_nat = pd.DataFrame(sums_by_period).reset_index()

    sum_nat = pd.merge(sum_nat, sum_nat_mom, on = ['supersector_name','industry_name','year','Month'])


    sum_nat['value_ytd_avg'] = pd.rolling_mean(sum_nat['value'],year_to_date)
    sum_nat['pct_change_ytd'] = sum_nat['value_ytd_avg'].pct_change(12)
    sum_nat['rank_ytd'] = ""
    sum_nat['job_growth_ytd'] = sum_nat['value_ytd_avg'].shift(12)*sum_nat['pct_change_ytd']
    sum_nat['pct_change_ytd'] = (sum_nat['pct_change_ytd']*100).round(2)
    sum_nat['job_growth_ytd'] =  sum_nat['job_growth_ytd'].round(2)

    sum_nat['value_ann_avg'] = pd.rolling_mean(sum_nat['value'],12)
    sum_nat['pct_change_ann'] = sum_nat['value_ann_avg'].pct_change(12)
    sum_nat['rank_ann'] = ""
    sum_nat['job_growth_ann'] = sum_nat['value_ann_avg'].shift(12)*sum_nat['pct_change_ann']
    sum_nat['pct_change_ann'] = (sum_nat['pct_change_ann']*100).round(2)
    sum_nat['job_growth_ann'] =  sum_nat['job_growth_ann'].round(2)

    sum_nat['rank'] = ""
    sum_nat['pct_change'] = sum_nat['value'].pct_change(12)
    sum_nat['job_growth'] = sum_nat['value'].shift(12)*sum_nat['pct_change']
    sum_nat['pct_change'] = (sum_nat['pct_change']*100).round(2)
    sum_nat['job_growth'] = sum_nat['job_growth'].round(2)
    sum_nat['area_name'] = "United States"
    sum_nat['state_name'] = "United States"

    sum_nat['pct_change_mom'] = sum_nat['value_mom'].pct_change(1)
    sum_nat['rank_mom'] = ""
    sum_nat['job_growth_mom'] = sum_nat['value_mom'].shift(1)*sum_nat['pct_change_mom']
    sum_nat['pct_change_mom'] = (sum_nat['pct_change_mom']*100).round(2)
    sum_nat['job_growth_mom'] = sum_nat['job_growth_mom'].round(2)

    sum_nat = sum_nat.astype(object).where(pd.notnull(sum_nat), None)

    print("************************  Created Final Tables: sum_nat")




    ######################################################################################################
    # All other regions, MSA & State

    # year over year pct_changes
    # Start here
    # From here we can calculate entirely new, pct_change and there by rank and job_growth
    # Wew calculate the percentage change or year to date or 12 month moving average
    # 12 month moving average of what? monthly changes in job growth? seasonally adjusted data?

    # Year to date percentage changes are going to be the percentage change of the current amount of month's we have experienced


############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#
    sums_states_mom = sm_table_final_v3.query('area_name == "Statewide"')
    group_g2 = sums_states_mom.groupby(['state_name','supersector_name','industry_name','year','Month'])
    sums_by_state = group_g2['value'].sum()
    sums_states_mom = pd.DataFrame(sums_by_state).reset_index()
    sums_states_mom.columns = ['state_name','supersector_name','industry_name','year','Month','value_mom']

    sums_states = sm_table_final_v2.query('area_name == "Statewide"')
    group_g2 = sums_states.groupby(['state_name','supersector_name','industry_name','year','Month'])
    sums_by_state = group_g2['value'].sum()
    sums_states = pd.DataFrame(sums_by_state).reset_index()

    sums_states = pd.merge(sums_states, sums_states_mom, on = ["state_name","supersector_name","industry_name","year","Month"], how = "left")

    sums_states['pct_change_mom'] = sums_states['value_mom'].pct_change(1)
    sums_states['rank_mom']       = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False, method = 'first')
    sums_states['job_growth_mom'] = sums_states['value_mom'].shift(1)*sums_states['pct_change_mom']
    sums_states['pct_change_mom'] = (sums_states['pct_change_mom']*100).round(2)
    sums_states['job_growth_mom'] = sums_states['job_growth_mom'].round(2)

    sums_states['value_ytd_avg'] = pd.rolling_mean(sums_states['value'], year_to_date)
    sums_states['pct_change_ytd'] = sums_states['value_ytd_avg'].pct_change(12)
    sums_states['rank_ytd'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
    sums_states['job_growth_ytd'] = sums_states['value_ytd_avg'].shift(12)*sums_states['pct_change_ytd']
    sums_states['pct_change_ytd'] = (sums_states['pct_change_ytd']*100).round(2)
    sums_states['job_growth_ytd'] =  sums_states['job_growth_ytd'].round(2)

    sums_states['value_ann_avg'] = pd.rolling_mean(sums_states['value'], 12)
    sums_states['pct_change_ann'] = sums_states['value_ann_avg'].pct_change(12)
    sums_states['rank_ann'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
    sums_states['job_growth_ann'] = sums_states['value_ann_avg'].shift(12)*sums_states['pct_change_ann']
    sums_states['pct_change_ann'] = (sums_states['pct_change_ann']*100).round(2)
    sums_states['job_growth_ann'] =  sums_states['job_growth_ann'].round(2)

    sums_states['pct_change'] = sums_states['value'].pct_change(12)
    sums_states['rank'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
    sums_states['job_growth'] = sums_states['value'].shift(12)*sums_states['pct_change']
    sums_states['pct_change'] = (sums_states['pct_change']*100).round(2)
    sums_states['job_growth'] = sums_states['job_growth'].round(2)

    print("************************  Created Final Tables: sums_states")

############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#


    def msas():
        # # ############################################################################################################
        # # # sums_msa to db
        sums_msa_mom = sm_table_final_v3.query('area_name != "Statewide"')
        group_g2 = sums_msa_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_by_msa = group_g2['value'].sum()
        sums_msa_mom = pd.DataFrame(sums_by_msa).reset_index()
        sums_msa_mom.columns = ['area_name','supersector_name','industry_name','year','Month','value_mom']

        sums_msa = sm_table_final_v2.query('area_name != "Statewide"')
        group_g3 = sums_msa.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_by_msa = group_g3['value'].sum()
        sums_msa = pd.DataFrame(sums_by_msa).reset_index()


        #Left merge needs to happen here. Need to include all industries from sums_msa because sums_msa_mom only includes Total Nonfarm
        sums_msa = pd.merge(sums_msa, sums_msa_mom, on = ['area_name','supersector_name','industry_name','year','Month'], how = "left")

        sums_msa['pct_change_mom'] = sums_msa['value_mom'].pct_change(1)
        sums_msa['rank_mom']       = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_mom'] = sums_msa['value_mom'].shift(1)*sums_msa['pct_change_mom']
        sums_msa['pct_change_mom'] = (sums_msa['pct_change_mom']*100).round(2)
        sums_msa['job_growth_mom'] = sums_msa['job_growth_mom'].round(2)

        sums_msa['value_ytd_avg'] = pd.rolling_mean(sums_msa['value'], year_to_date)
        sums_msa['pct_change_ytd'] = sums_msa['value_ytd_avg'].pct_change(12)
        sums_msa['rank_ytd'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_ytd'] = sums_msa['value_ytd_avg'].shift(12)*sums_msa['pct_change_ytd']
        sums_msa['pct_change_ytd'] = (sums_msa['pct_change_ytd']*100).round(2)
        sums_msa['job_growth_ytd'] =  sums_msa['job_growth_ytd'].round(2)

        sums_msa['value_ann_avg'] = pd.rolling_mean(sums_msa['value'], 12)
        sums_msa['pct_change_ann'] = sums_msa['value_ann_avg'].pct_change(12)
        sums_msa['rank_ann'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_ann'] = sums_msa['value_ann_avg'].shift(12)*sums_msa['pct_change_ann']
        sums_msa['pct_change_ann'] = (sums_msa['pct_change_ann']*100).round(2)
        sums_msa['job_growth_ann'] =  sums_msa['job_growth_ann'].round(2)

        sums_msa['pct_change'] = sums_msa['value'].pct_change(12)
        sums_msa['rank'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa['job_growth'] = sums_msa['value'].shift(12)*sums_msa['pct_change']
        sums_msa['pct_change'] = (sums_msa['pct_change']*100).round(2)
        sums_msa['job_growth'] = sums_msa['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa")


    def msas_over():
        # ###############################################################################################################################

        # # The following code munges the MSA over & Starting at the sums dataframe it moves forward, selects the list of MSA's (total
        # # Non farm over 1000) and then filters on those MSA's and applies the functionality we have been using for pct changes and Rankings


############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#
        sums_msa_over_mom = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value > 1000)')
        msas_over = list(sums_msa_over_mom['area_name'].unique())
        sums_msa_over_mom = sm_table_final_v3[sm_table_final_v3['area_name'].isin(msas_over)]
        group_msas = sums_msa_over_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_over_mom = group_msas['value'].sum()
        sums_msa_over_mom = pd.DataFrame(sums_msa_over_mom).reset_index()
        sums_msa_over_mom.columns = ['area_name','supersector_name','industry_name','year','Month', 'value_mom']


        sums_msa_over = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value > 1000)')
        msas_over = list(sums_msa_over['area_name'].unique())
        sums_msa_over = sm_table_final_v3[sm_table_final_v3['area_name'].isin(msas_over)]
        group_msas = sums_msa_over.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_over = group_msas['value'].sum()
        sums_msa_over = pd.DataFrame(sums_msa_over).reset_index()

        sums_msa_over = pd.merge(sums_msa_over, sums_msa_over_mom,  on = ['area_name','supersector_name','industry_name','year','Month'], how = "left")


        sums_msa_over['pct_change_mom'] = sums_msa_over['value_mom'].pct_change(1)
        sums_msa_over['rank_mom'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False)
        sums_msa_over['job_growth_mom'] = sums_msa_over['value_mom'].shift(1)*sums_msa_over['pct_change_mom']
        sums_msa_over['pct_change_mom'] = (sums_msa_over['pct_change_mom']*100).round(2)
        sums_msa_over['job_growth_mom'] = sums_msa_over['job_growth_mom'].round(2)

        sums_msa_over['value_ytd_avg'] = pd.rolling_mean(sums_msa_over['value'],year_to_date)
        sums_msa_over['pct_change_ytd'] = sums_msa_over['value_ytd_avg'].pct_change(12)
        sums_msa_over['rank_ytd'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth_ytd'] = sums_msa_over['value_ytd_avg'].shift(12)*sums_msa_over['pct_change_ytd']
        sums_msa_over['pct_change_ytd'] = (sums_msa_over['pct_change_ytd']*100).round(2)
        sums_msa_over['job_growth_ytd'] =  sums_msa_over['job_growth_ytd'].round(2)

        sums_msa_over['value_ann_avg'] = pd.rolling_mean(sums_msa_over['value'], 12)
        sums_msa_over['pct_change_ann'] = sums_msa_over['value_ann_avg'].pct_change(12)
        sums_msa_over['rank_ann'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth_ann'] = sums_msa_over['value_ann_avg'].shift(12)*sums_msa_over['pct_change_ann']
        sums_msa_over['pct_change_ann'] = (sums_msa_over['pct_change_ann']*100).round(2)
        sums_msa_over['job_growth_ann'] =  sums_msa_over['job_growth_ann'].round(2)

        sums_msa_over['pct_change'] = sums_msa_over['value'].pct_change(12)
        sums_msa_over['rank'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth'] = sums_msa_over['value'].shift(12)*sums_msa_over['pct_change']
        sums_msa_over['pct_change'] = (sums_msa_over['pct_change']*100).round(2)
        sums_msa_over['job_growth'] = sums_msa_over['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa_over")


    def msas_under():

############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#
        sums_msa_under_mom = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value < 1000)')
        group_msas = sums_msa_under_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_under_mom = group_msas['value'].sum()
        sums_msa_under_mom = pd.DataFrame(sums_msa_under_mom).reset_index()
        sums_msa_under_mom.columns = ['area_name','supersector_name', 'industry_name', 'year', 'Month', 'value_mom']

        sums_msa_under = sm_table_final_v2.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value < 1000)')
        group_msas = sums_msa_under.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_under = group_msas['value'].sum()
        sums_msa_under = pd.DataFrame(sums_msa_under).reset_index()


        sums_msa_under = pd.merge(sums_msa_under, sums_msa_under_mom, how = "left", on = ['area_name','supersector_name', 'industry_name', 'year', 'Month'])


        sums_msa_under['pct_change_mom'] = sums_msa_under['value_mom'].pct_change(1)
        sums_msa_under['rank_mom'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False)
        sums_msa_under['job_growth_mom'] = sums_msa_under['value_mom'].shift(1)*sums_msa_under['pct_change_mom']
        sums_msa_under['pct_change_mom'] = (sums_msa_under['pct_change_mom']*100).round(2)
        sums_msa_under['job_growth_mom'] = sums_msa_under['job_growth_mom'].round(2)

        sums_msa_under['value_ytd_avg'] = pd.rolling_mean(sums_msa_under['value'],year_to_date)
        sums_msa_under['pct_change_ytd'] = sums_msa_under['value_ytd_avg'].pct_change(12)
        sums_msa_under['rank_ytd'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth_ytd'] = sums_msa_under['value_ytd_avg'].shift(12)*sums_msa_under['pct_change_ytd']
        sums_msa_under['pct_change_ytd'] = (sums_msa_under['pct_change_ytd']*100).round(2)
        sums_msa_under['job_growth_ytd'] =  sums_msa_under['job_growth_ytd'].round(2)

        sums_msa_under['value_ann_avg'] = pd.rolling_mean(sums_msa_under['value'], 12)
        sums_msa_under['pct_change_ann'] = sums_msa_under['value_ann_avg'].pct_change(12)
        sums_msa_under['rank_ann'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth_ann'] = sums_msa_under['value_ann_avg'].shift(12)*sums_msa_under['pct_change_ann']
        sums_msa_under['pct_change_ann'] = (sums_msa_under['pct_change_ann']*100).round(2)
        sums_msa_under['job_growth_ann'] =  sums_msa_under['job_growth_ann'].round(2)

        sums_msa_under['pct_change'] = sums_msa_under['value'].pct_change(12)
        sums_msa_under['rank'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth'] = sums_msa_under['value'].shift(12)*sums_msa_under['pct_change']
        sums_msa_under['pct_change'] = (sums_msa_under['pct_change']*100).round(2)
        sums_msa_under['job_growth'] = sums_msa_under['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa_under")


############################################################################################################################
# The purpose of this section...
#
#
#
#
#
#


# # ###########################################################################################
# # # Writing data and deleting frames we don't need

    engine = create_engine("mysql+mysqldb://job_growthmaster:jobgrowthpass@job-growth.cotzely14ram.us-west-2.rds.amazonaws.com/job_growth2?charset=utf8&use_unicode=0")

    sum_nat.to_sql('national_rankings_t', engine, flavor='mysql', if_exists='replace')
# # #sum_nat.to_csv("national_rankings.csv", index = False)

# # sum_nat_mom.to_sql('national_rankings_mom', engine, flavor='mysql', if_exists='replace')
# # #sum_nat_mom.to_csv("national_rankings_mom.csv", index = False)

    sums_states.to_sql('state_rankings_t', engine, flavor='mysql', if_exists='replace')
# # #sums_states.to_csv("state_rankings.csv", index = False)

# #sums_msa.to_sql('msa_rankings_t', engine, flavor='mysql', if_exists='replace')
# # #sums_msa.to_csv("msa_rankings.csv", index = False)

# # #sm_table_final_v3.to_sql('sm_table_final_v3', engine, flavor='mysql', if_exists='replace')
# # #sm_table_final_v3.to_csv("sm_table_final_v3.csv")

# # #sm_table_final_v2.to_sql('sm_table_final_v2', engine, flavor='mysql', if_exists='replace')
# # #sm_table_final_v2.to_csv("sm_table_final_v2.csv")

# # sums_msa_over_mom.to_sql('msa_rankings_over_mom', engine, flavor='mysql', if_exists='replace')
# # #sums_msa_over_mom.to_csv("msa_rankings_over_mom.csv",index = False)

# # sums_msa_under_mom.to_sql('msa_rankings_under_mom', engine, flavor='mysql', if_exists='replace')
# # #sums_msa_under_mom.to_csv("msa_rankings_under_mom.csv",index = False)

# # sums_msa_under.to_sql('msa_rankings_under', engine, flavor='mysql', if_exists='replace')
# # #sums_msa_under.to_csv("msa_rankings_under.csv",index = False)

# # sums_msa_over.to_sql('msa_rankings_over', engine, flavor='mysql', if_exists='replace')
# # #sums_msa_over.to_csv("msa_rankings_over.csv",index = False)


# # del sum_nat
# # del sum_nat_mom

# # del sums_states
# # del sums_states_mom

# # del sums_msa
# # del sums_msa_mom

# # del sums_msa_over
# # del sums_msa_under

# # del sums_msa_over_mom
# # del sums_msa_under_mom


finally:
    filepath = os.path.join( os.path.dirname( __file__ ), '..' )
    os.chdir(filepath)




