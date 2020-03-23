# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 17:36:04 2020

@author: 1000003613
"""

import pandas as pd
from datetime import datetime
import pyodbc
from dateutil.relativedelta import relativedelta

con_94=pyodbc.connect(driver='{SQL Server}',server = '172.30.21.94', database = 'MMFSL_Sandbox',UID= 'TAB_WH', PWD='TAB_WH' )#Follow the order


prev_month = (datetime.now()+relativedelta(months=-1)).strftime('%b')

current_month = datetime.now().strftime('%b')

year=datetime.now().strftime('%Y')

first_three=['Dec','Jan','Feb','Mar']
if prev_month in first_three:
    fy_period='FY'+datetime.now().strftime('%y')
else:
    fy_period='FY'+((datetime.now()+relativedelta(years=+1)).strftime('%y'))

#Latest Closing Age
def necessary_columns_all(prev_month,fy_period):
    qry1="SELECT NEW_HPANO, "+prev_month+"_AGE, "+prev_month+"_STATUS, FY_Period "\
    "FROM " + "[MMFSL_SandBox].[dbo].[GP_TB_Details]  \
    WHERE FY_Period IN ('" + fy_period +"')"

    monthend_closing_age_all_contracts = pd.read_sql(qry1,con_94)
    #print (closing_age.shape)
    return monthend_closing_age_all_contracts

closing_all=necessary_columns_all(prev_month,fy_period)
closing_all.to_csv('C:/Users/1000003613/Desktop/Data Collection model26Mar19/Automation/automate/month_end_closing_age_fetch/closing_age_all_statuses.csv' , index=False)

def necessary_columns_LU(prev_month,fy_period):
    qry1="SELECT NEW_HPANO, "+prev_month+"_AGE, "+prev_month+"_STATUS, FY_Period "\
    "FROM " + "[MMFSL_SandBox].[dbo].[GP_TB_Details]  \
    WHERE "+prev_month+"_STATUS IN ('L','U') AND FY_Period IN ('" + fy_period +"')"

    monthend_closing_age_L_U_contracts = pd.read_sql(qry1,con_94)
    #print (closing_age.shape)
    return monthend_closing_age_L_U_contracts

closing_lu=necessary_columns_LU(prev_month,fy_period)
#closing_lu.to_csv('C:/Users/1000003613/Desktop/Data Collection model26Mar19/Automation/automate/month_end_closing_age_fetch/closing_age_L_U_contracts.csv' , index=False)
