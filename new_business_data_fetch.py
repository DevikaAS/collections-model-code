# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 10:09:14 2020

@author: 1000003613
"""

import pyodbc
import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta


a=datetime.now()+relativedelta(months=-1,day=1)
start_date=a.date().strftime("%Y-%m-%d")
#master_data_with_demographics_details
#con_94 Analytics DataMart
con_94=pyodbc.connect('Trusted_Connection = yes',driver='{SQL Server}',server = '172.30.21.94', database = 'MMFSL_Sandbox',UID= 'TAB_WH', PWD='TAB_WH')#Follow the order

Static_New_Business_Data = "SELECT Stat.[NEW_HPANO]\
,Stat.[TENURE]\
,Stat.[STATE_DES]\
,Stat.[INV_VALUE]\
,Stat.[TALUK]\
,Stat.[FIN_AMOUNT]\
,Stat.[BRANCH]\
,Stat.[FA_DEALER]\
,Stat.[PRDCTY]\
,Stat.[REFINANCE]\
,Stat.[MANUDES]\
,Stat.[FIN_CHARGE]\
,Stat.[acct_yyyymm] AS [ACCT_YYYYMM]\
,Stat.[NOOFINSTL]\
,Stat.[ASSDES]\
,Stat.[HPA_DATE]\
FROM [MMFSL_SandBox].[dbo].[GP_Dctran_Static_Dtls] AS Stat \
where Stat.[HPA_DATE] >= '"+start_date+"'\
order by Stat.[NEW_HPANO];"

Static_New_Business_Data=pd.read_sql(Static_New_Business_Data,con_94)


Demographics_Data = "SELECT Demo.[NEW_HPANO]\
,Demo.[AGE]\
,Demo.[GENDER]\
,Demo.[MARITAL_STATUS]\
,Demo.[PAN_STATUS]\
,Demo.[NO_OF_DEPENDENTS]\
,Demo.[TRACTOR_OWNERSHIP]\
,Demo.[HOUSE_OWNERSHIP]\
,Demo.[NO_OF_EARNING_MEMBERS]\
FROM [MMFSL_SandBox].[dbo].[GP_DWH_Demographic_Dtls] AS Demo \
order by Demo.[NEW_HPANO];"

Demographics_Data=pd.read_sql(Demographics_Data,con_94)

TB_Extn_Dtls_Data = "SELECT TB.[NEW_HPANO]\
,TB.[MIS_SECTOR]\
,TB.[MIS_PRODUCT]\
,TB.[COLL_VERTICAL]\
,TB.[HANDLING_FUNCTION]\
FROM [MMFSL_SandBox].[dbo].[TB_Extn_Dtls] AS TB \
order by TB.[NEW_HPANO];"

TB_Extn_Dtls_Data=pd.read_sql(TB_Extn_Dtls_Data,con_94)

Basic_Dtls_Data = "SELECT B.[NEW_HPANO]\
,B.[GUARANTOR_POSSIBLE]\
,B.[REPAYMENT_MODE]\
,B.[APPX_INSTALLMENT_AMT]\
FROM [MMFSL_SandBox].[dbo].[GP_DWH_Basic_Dtls] AS B \
order by B.[NEW_HPANO];"

Basic_Dtls_Data=pd.read_sql(Basic_Dtls_Data,con_94)

Static_New_Business_Data['NEW_HPANO']=Static_New_Business_Data['NEW_HPANO'].astype(str)
Demographics_Data['NEW_HPANO']=Demographics_Data['NEW_HPANO'].astype(str)
TB_Extn_Dtls_Data['NEW_HPANO']=TB_Extn_Dtls_Data['NEW_HPANO'].astype(str)
Basic_Dtls_Data['NEW_HPANO']=Basic_Dtls_Data['NEW_HPANO'].astype(str)

merge0=pd.merge(Static_New_Business_Data,TB_Extn_Dtls_Data,how='left',on='NEW_HPANO')

merge1=pd.merge(Static_New_Business_Data,Demographics_Data,how='left',on='NEW_HPANO')
merge1=merge1[['NEW_HPANO','AGE','GENDER','MARITAL_STATUS', 'PAN_STATUS', 'NO_OF_DEPENDENTS','TRACTOR_OWNERSHIP', 'HOUSE_OWNERSHIP', 'NO_OF_EARNING_MEMBERS']]

merge2=pd.merge(Static_New_Business_Data,Basic_Dtls_Data,how='left',on='NEW_HPANO')
#merge2.columns.values
merge2=merge2[['NEW_HPANO','GUARANTOR_POSSIBLE','REPAYMENT_MODE','APPX_INSTALLMENT_AMT']]

merge3=pd.merge(merge0,merge1,how='left',on='NEW_HPANO')
merge3.columns.values
merge4=pd.merge(merge3,merge2,how='left',on='NEW_HPANO')


merge4.to_csv('C:/Users/1000003613/Desktop/Data Collection model26Mar19/Automation/automate/new_business_data_fetch/new_business_data.csv' , index=False)




