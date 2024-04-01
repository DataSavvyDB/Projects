# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 14:16:28 2023

@author: deebhatt
"""
#################*********************************************  AO GL Details + BPC Data  ********************************************############

# Importing libraries
import pandas as pd;
import pyodbc as dbc;
import numpy as np;

# Reading BPC data from local file
conn = dbc.connect('''DRIVER={SQL Server};  Server=USHDC9854; UID=BPC_LA; PWD=TKyoh.ZF; DataBase=Curriculum_Planning''');
query_BPC = "SELECT fc.*, t.[PERIOD],t.[YEAR],t.[ID],p.EVDESCRIPTION,p.LEARNING_AREA,p.NSPENDTYPE,o.CO_CODE,o.REGION FROM [dbo].[vw_TBLFACTCOST_PLANNING] fc LEFT JOIN mbrTime t ON t.TIMEID = fc.TIMEID LEFT JOIN mbrproject p ON p.ID = fc.PROJECT LEFT JOIN mbrorganization o ON p.COSTCENTER = o.ID WHERE CASE WHEN t.[YEAR] = 2023 AND fc.CATEGORY IN ('FORECAST','MICRO_FINAL') AND fc.ORGANIZATION != 180000231 THEN 'Keep' WHEN t.[YEAR] = 2023 AND fc.CATEGORY IN ('FORECAST','MICRO_FINAL','ACTUAL') AND fc.ORGANIZATION = 180000231 THEN 'Keep' WHEN t.[YEAR] = 2024 AND fc.CATEGORY IN ('Forecast_P6','MICRO_FINAL') AND fc.ORGANIZATION != 180000231 THEN 'Keep' WHEN t.[YEAR] = 2024 AND fc.CATEGORY IN ('Forecast_P6','MICRO_FINAL','ACTUAL') AND fc.ORGANIZATION = 180000231 THEN 'Keep' END = 'Keep'";
query_ST = "SELECT REPLACE([SAPID], '-','') AS [Assignment Number], NSPENDTYPE AS [Spend Type] FROM mbrproject WHERE SAPID IS NOT NULL";
BPC_CW = pd.read_sql_query(query_ST, conn);
BPC_CW = BPC_CW.drop_duplicates();
BPC = pd.read_sql_query(query_BPC, conn);

#Creating Blank columns for matching required format
Vendor_Header = " ";
BPC['Vendor Header'] = Vendor_Header;
Vendor_Header_Description = " ";
BPC['Vendor Header Description'] = Vendor_Header_Description;
Fiscal_Period_Sorting = " ";
BPC['Fiscal Period Sorting'] = Fiscal_Period_Sorting;
Expense_Group = " ";
BPC['Expense Group'] = Expense_Group;
DataSource = "BPC Data";
BPC['Data Source'] = DataSource;

# Renaming columns for matching required format
BPC.rename(columns = {'YEAR':'Ledger Fiscal Year', 'ACCOUNT':'GL Account', 'ORGANIZATION':'CostCenter', 'REGION':'US / USI', 'EVDESCRIPTION':'WBS Description', 'SIGNEDDATA':'Amount', 'NSPENDTYPE':'Spend Type', 'ID':'Time.ID'}, inplace = True);

# Creating Period columns by removing word "Period" from values and Renaming column name
BPC['PERIOD'] = BPC['PERIOD'].replace("Period ","", regex = True);
BPC.rename(columns = {'PERIOD':'Fiscal Period'}, inplace = True);

# Creating WBS Code and Assignment Number from Project and Renaming column name
BPC['WBS Code'] = BPC['PROJECT'].replace("_","-", regex = True);
BPC['Assignment Number'] = BPC['PROJECT'].replace("_","", regex = True);

# Removing columns that are not required from BPC data
AO_BPC_comb = BPC.drop(['PROJECT','DATASOURCE','RPTCURRENCY','SOURCE','TIMEID','LEARNING_AREA','CO_CODE'], axis=1);

# Reordering BPC columns as per required format
cols = ['Ledger Fiscal Year','Fiscal Period','GL Account','CostCenter','US / USI','WBS Description','WBS Code','Time.ID','Assignment Number','Vendor Header','Vendor Header Description','Amount','CATEGORY','Spend Type','Data Source'];
AO_BPC_comb = AO_BPC_comb[cols];

# Reading WBS, GL Account Crosswalk and AO data from local file
GL_CW = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\AO_Actuals_FY24_P06.xlsx', sheet_name='Account Crosswalk'); # GL_CW --> Account Crosswalk
AO_23 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY23\AO_Actuals_FY23.xlsx', sheet_name='details');
# AO_24 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\AO_Actuals_FY24.xlsx', sheet_name='details', header = 24);
AO_24 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\AO_Actuals_FY24_P06.xlsx', sheet_name='details');
LA_CW = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\Learning Area Mapping.xlsx', sheet_name='Sheet1');
Period_CW = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\Period & Quarter Mapping.xlsx', sheet_name='Sheet1');
#CostCenterAO24 = AO_24['CostCenter'].str.slice(5,14);
CostCenterAO24 = AO_24['CostCenter'];
CostCenterAO23 = AO_23['CostCenter'];
CostCenter_Concat = pd.concat([CostCenterAO23,CostCenterAO23]);
AO_24['CostCenter'] = CostCenterAO24;
AO_24 = AO_24.astype({'CostCenter':'int64'});
AO_24 = AO_24.astype({'Unassigned':'object'});
AO_comb = pd.concat([AO_23,AO_24]);

# Merging AO Data with BPC_CW 
AO_comb = pd.merge(AO_comb, BPC_CW, on='Assignment Number', how='left');
AO_comb.drop(columns=['Design/Delivery'], inplace=True);

# Creating Time.ID in AO to match the format with AO_BPC_comb
Time_ID = " ";
AO_comb['Time.ID'] = Time_ID;
Learning_Area = " ";
AO_comb['Learning Area']=Learning_Area;
CATEGORY = "ACTUAL";
AO_comb['CATEGORY'] = CATEGORY;
AO_comb.rename(columns = {'US / USI':'US/USI','Account Group':'Account Groupings','P&A Groups':'P&A Groupings','Vendor-Header':'Vendor Header','Vendor-Header Description':'Vendor Header Description'}, inplace = True);
AO_comb['Expense Group'] = Expense_Group;
AO_comb['Fiscal Period Sorting'] = Fiscal_Period_Sorting;
DataSourceAO = "AO GL Details";
AO_comb['Data Source'] = DataSourceAO;
AO_comb = AO_comb.astype({'CostCenter':'int64'});

# Creating sub dataframes to merge Attributes from AO to BPC 
AO_BL = AO_comb[['CostCenter','Business Line']].copy();
AO_BL = AO_BL.drop_duplicates();
AO_US_USI = AO_comb[['CostCenter','US/USI']].copy();
AO_US_USI = AO_US_USI.drop_duplicates();
AO_CC_Desc23 = AO_23[['CostCenter','CostCenter Description']].copy();
AO_CC_Desc23 = AO_CC_Desc23.drop_duplicates();
AO_CC_Desc24 = AO_24[['CostCenter','CostCenter Description']].copy();
AO_CC_Desc24 = AO_CC_Desc24.drop_duplicates();


# Renaming columns in Account and WBS Crosswalks
GL_CW.rename(columns = {'G/L Account':'GL Account', 'G/L Account Description':'GL Account Description'}, inplace = True);

# Reordering Account Crosswalk columns as per required format
cols_GL_CW = ['GL Account', 'GL Account Description','Account Groupings','P&A Groupings','Firm Projects Groupings'];
GL_CW = GL_CW[cols_GL_CW];

# Reordering AO columns as per required format
cols_AO = ['Ledger Fiscal Year','Fiscal Period','GL Account','CostCenter Description','CostCenter','Business Line','Learning Area','US/USI','WBS Description','WBS Code','Time.ID','Assignment Number','Vendor Header','Vendor Header Description','Amount', 'CATEGORY','Spend Type','Fiscal Period Sorting','Expense Group','Data Source'];
AO_comb = AO_comb[cols_AO];

# Merging All data frames to create BPC data in final required format
AO_BPC_comb.drop(AO_BPC_comb[AO_BPC_comb['GL Account'] == 'PROJ_ACTIVE'].index, inplace = True);
AO_BPC_comb = AO_BPC_comb.astype({'GL Account':'int64'});
AO_BPC_comb = AO_BPC_comb.astype({'CostCenter':'int64'});
AO_BPC_comb = AO_BPC_comb.merge(AO_BL, how='left', on='CostCenter');
AO_BPC_comb = AO_BPC_comb.merge(AO_US_USI, how='left', on='CostCenter');
AO_BPC_comb = pd.merge(AO_BPC_comb, AO_CC_Desc24, on='CostCenter', how='left');
AO_BPC_comb = pd.merge(AO_BPC_comb, AO_CC_Desc23, how= 'left', on = 'CostCenter');
AO_BPC_comb['CostCenter Description_x'].fillna(AO_BPC_comb['CostCenter Description_y'], inplace=True);
AO_BPC_comb.drop(columns=['CostCenter Description_y'], inplace=True);
AO_BPC_comb.rename(columns={'CostCenter Description_x': 'CostCenter Description'}, inplace=True);



# Fill na values
AO_BPC_comb['US/USI'].fillna(AO_BPC_comb['US / USI'], inplace = True);
AO_BPC_comb.drop(columns = ['US / USI'], inplace = True);

# Creating Blank columns for matching required format
AO_BPC_comb['Expense Group'] = Expense_Group;
AO_BPC_comb['Fiscal Period Sorting'] = Fiscal_Period_Sorting;

# Appending AO Actuals and BPC data
AO_BPC_comb = pd.concat([AO_BPC_comb,AO_comb]);
AO_BL = AO_comb[['CostCenter','Business Line']].copy();
AO_BL = AO_BL.drop_duplicates();

# AO BPC Comb merge with new learning Area
AO_BPC_comb = AO_BPC_comb.merge(LA_CW, how='left', on='Assignment Number');
AO_BPC_comb = AO_BPC_comb.drop(['Business'], axis=1);

# GL Crosswalk merge with data
AO_BPC_comb = AO_BPC_comb.merge(GL_CW, how='left', on='GL Account');

# Re-creating WBS Code column from Assignement number to match with BPC data for merging
AO_BPC_comb['WBS Code'] = AO_BPC_comb["Assignment Number"].str.slice(0,8)+"-"+AO_BPC_comb["Assignment Number"].str.slice(8,10)+"-"+AO_BPC_comb["Assignment Number"].str.slice(10,12)+"-"+AO_BPC_comb["Assignment Number"].str.slice(12,14)+"-"+AO_BPC_comb["Assignment Number"].str.slice(14,18);

# Converting datatypes for concatenation
AO_comb = AO_comb.astype({'Fiscal Period':'object','GL Account':'object',});

# Creating Fiscal Period Sorting column
AO_BPC_comb['Fiscal Period Sorting'] = AO_BPC_comb['Fiscal Period'];

# Reordering BPC columns as per required format
ncols_AO_BPC_comb = ['Ledger Fiscal Year','Fiscal Period','GL Account','GL Account Description','CostCenter Description','CostCenter','Business Line','Learning Area','Learning Group','US/USI','Project Name','WBS Description','WBS Code','Time.ID','Assignment Number','Vendor Header','Vendor Header Description','Amount','CATEGORY','Spend Type','Account Groupings','P&A Groupings','Firm Projects Groupings','Fiscal Period Sorting','Expense Group','Data Source'];
AO_BPC_comb = AO_BPC_comb[ncols_AO_BPC_comb];

# Changing Business line for DLHP Cost Centers
conditions = [(AO_BPC_comb['CostCenter'] == 180045788), (AO_BPC_comb['CostCenter'] == 180045789), (AO_BPC_comb['CostCenter'] == 110270195), (AO_BPC_comb['CostCenter'] == 180000231)];
values = ['DLHP','DLHP','DTA','GPS'];
AO_BPC_comb['Business Line'] = np.select(conditions, values, default = AO_BPC_comb['Business Line']);

# Changing Spend Type as per Cost Centers & Business Line  
condition_ST = [(AO_BPC_comb['CostCenter'] == 180008367), (AO_BPC_comb['CostCenter'] == 180002635), (AO_BPC_comb['CostCenter'] == 180049808), (AO_BPC_comb['CostCenter'] == 180042320),(AO_BPC_comb['Business Line'] == "Deloitte University")];
values_ST = ['Design','Delivery','Delivery','Design','Delivery'];
AO_BPC_comb['Spend Type'] = np.select(condition_ST, values_ST, default = AO_BPC_comb['Spend Type']);

# Standardizing US/USI
dict_US_USI = {'YES':'USI','NO':'US','US - National Office':'US','US - USI':'USI'};
AO_BPC_comb['US/USI'] = AO_BPC_comb['US/USI'].map(dict_US_USI);

dict_Category = {'MICRO_FINAL':'PLAN'};
AO_BPC_comb['CATEGORY'] = AO_BPC_comb['CATEGORY'].map(dict_Category).fillna(AO_BPC_comb['CATEGORY']);

# Concatenating columns
AO_BPC_comb['Concat'] = AO_BPC_comb.apply(lambda x:'%s_%s_%s' % (x['Ledger Fiscal Year'],x['CATEGORY'],x['Fiscal Period']),axis=1);


#################***************************************************  AO Front Page  ********************************************############

# Reading the data for FY23
AOFP_act23 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY23\First Page\Dashboard AO data_FY23 Plan and AFR.xlsx', sheet_name='Actuals_FY23');
AOFP_pln23 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY23\First Page\Dashboard AO data_FY23 Plan and AFR.xlsx', sheet_name='Plan_FY23');
AOFP_fct23 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY23\First Page\Dashboard AO data_FY23 Plan and AFR.xlsx', sheet_name='Forecast_FY23');

# Reading the data for FY24
AOFP_act24 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\First Page\Dashboard AO data_V1_F24_P06.xlsx', sheet_name='FY24 Actuals_P06');
AOFP_pln24 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\First Page\Dashboard AO data_V1_F24_P06.xlsx', sheet_name='FY24Plan');
AOFP_fct24 = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\First Page\Dashboard AO data_V1_F24_P06.xlsx', sheet_name='FY24 Forecast_P06');

# Dropping the blank header column for FY24
AOFP_act24 = AOFP_act24.drop(['Unnamed: 1'], axis=1);
AOFP_pln24 = AOFP_pln24.drop(['Unnamed: 1'], axis=1);
AOFP_fct24 = AOFP_fct24.drop(['Unnamed: 1'], axis=1);

# Dropping the blank header column for FY23
AOFP_act23 = AOFP_act23.drop(['Column2'], axis=1);
AOFP_pln23 = AOFP_pln23.drop(['Column2'], axis=1);
AOFP_fct23 = AOFP_fct23.drop(['Column2'], axis=1);

# Adding the category for all three data sets 'Plan' 'Actuals' and 'Forecast' for all Fiscal years
cat_act = "Actuals";
cat_pln = "Plan";
cat_fct = "Forecast";
AOFP_act24['Category'] = cat_act;
AOFP_pln24['Category'] = cat_pln;
AOFP_fct24['Category'] = cat_fct;
AOFP_act23['Category'] = cat_act;
AOFP_pln23['Category'] = cat_pln;
AOFP_fct23['Category'] = cat_fct;

# Adding Renaming columns as Amount
AOFP_act24.rename(columns = {'Actuals':'Amount'}, inplace = True);
AOFP_pln24.rename(columns = {'Plan':'Amount'}, inplace = True);
AOFP_fct24.rename(columns = {'Forecast':'Amount'}, inplace = True);
AOFP_act23.rename(columns = {'Actuals':'Amount'}, inplace = True);
AOFP_pln23.rename(columns = {'Plan':'Amount'}, inplace = True);
AOFP_fct23.rename(columns = {'Forecast':'Amount'}, inplace = True);

# Concatenating All the individuals datasets
AOFP_APF = pd.concat([AOFP_act23,AOFP_pln23,AOFP_fct23,AOFP_act24,AOFP_pln24,AOFP_fct24]);

# Creating Grouping for Firm Projects and other categories
AOFP_APF['Cat']=AOFP_APF.Summary.str.match('Firm Proj');
dict_map = {'Earnings Before Allocations':'Net Costs','Administrative Salaries Incurred':'Administrative Salaries Incurred','Prof & Admin Expenses Incurred':'Prof & Admin Expenses Incurred','Total Headcount':'Total Headcount','Occupancy Expenses Incurred':'Other','Provision For Bad Debts':'Other','Interest On Receivables':'Other','Organization-wide Costs':'Other','Exchange Rate Differential':'Other','Gain/Loss on Divestiture':'Other','Gain/Loss on Divestiture':'Other','Interest Credit':'Other','Claims and Insurance Costs':'Other','Direct Margin':'Other'};
AOFP_APF['Grouping'] = AOFP_APF['Summary'].map(dict_map);
AOFP_APF.loc[AOFP_APF['Cat'] == True, 'Grouping'] = 'Firm Project Costs';
AOFP_APF = AOFP_APF.drop(['Cat'], axis=1);
AOFP_APF['Expense Group'] = AOFP_APF['Summary'];
AOFP_APF = AOFP_APF[AOFP_APF['Grouping'].notna()];
AOFP_APF = AOFP_APF[AOFP_APF['Expense Group'].str.contains("Firm Project Costs|Firm Projects - Admin") == False];

# Mapping US and USI
dict_US_USI = {'YES':'USI','NO':'US'};
AOFP_APF['PC USI'] = AOFP_APF['PC USI'].map(dict_US_USI);

# Cleaning Profit Center
AOFP_APF['Profit Center'] = AOFP_APF['Profit Center'].str.slice(6,15);

# Converting Amount as per requirement (EBA*-1, Remaining*1)
AOFP_APF['Amount'] = np.where(AOFP_APF['Summary'] == 'Earnings Before Allocations', AOFP_APF['Amount'] * -1, AOFP_APF['Amount'] * 1);
AOFP_APF['Amount'] = np.where(AOFP_APF['Summary'] == 'Direct Margin', AOFP_APF['Amount'] * -1, AOFP_APF['Amount'] * 1);


# Cleaning Fiscal Year as per requirement
dict_FY = {'FY- Deloitte SWIFT 2024':'2024','Deloitte SWIFT 2023':'2023'};
AOFP_APF['Fiscal Year'] = AOFP_APF['Fiscal Year'].map(dict_FY);

# Dropping Summary column
AOFP_APF = AOFP_APF.drop(['Summary'], axis=1);

# Mapping Sort Order Column for Grouping
dict_EGSO = {'Administrative Salaries Incurred':'1','Firm Project Costs':'2','Prof & Admin Expenses Incurred':'3','Other':'4','Earnings Before Allocations':'5','Total Headcount':'4','Net Costs':'5'};
AOFP_APF['Expense Group Sort Order'] = AOFP_APF['Grouping'].map(dict_EGSO);

# Changing Business line for DLHP Cost Centers
conditions = [(AOFP_APF['Profit Center'] == "180045788"), (AOFP_APF['Profit Center'] == "180045789"), (AOFP_APF['Profit Center'] == "110270195"), (AOFP_APF['Profit Center'] == "180000231")];
values = ['DLHP','DLHP','DTA','GPS'];
AOFP_APF['PC Business Line'] = np.select(conditions, values, default = AOFP_APF['PC Business Line']);

# Adding Identifier Column for AO Front Page data
conditions_DS = [(AOFP_APF['Grouping'] == "Total Headcount")];
values_DS = ['Headcount Front Page'];
AOFP_APF['Data Source'] = np.select(conditions_DS, values_DS, default = "AO APF Data Front Page");

# Mapping Sort Order Column for Business Line
dict_BLSO = {'Tax':'1','RFA':'2','Consulting':'3','A&A':'4','EA':'5','DLHP':'6','DTA':'7','L&D Leadership':'8','Deloitte University':'9'};
AOFP_APF['Business Line Sort Order'] = AOFP_APF['PC Business Line'].map(dict_BLSO);

# Rearranging columns for AOFP_APF
cols = ['Fiscal Year','Fiscal Period','Profit Center','PC Business Line','PC USI','Expense Group','Amount','Category','Grouping','Expense Group Sort Order','Business Line Sort Order','Data Source'];
AOFP_APF = AOFP_APF[cols];

# Renaming Columns of AOFP_APF as per standard names
AOFP_APF.rename(columns = {'Fiscal Year':'Ledger Fiscal Year','Profit Center':'CostCenter','PC Business Line':'Business Line','PC USI':'US/USI','Category':'CATEGORY'}, inplace = True);

# Adding Blank columns to match the Format
GL_Account = " ";
AOFP_APF['GL_Account'] = GL_Account;
GL_Account_Description = " ";
AOFP_APF['GL Account Description'] = GL_Account_Description;
CostCenter_Description = " ";
AOFP_APF['CostCenter Description'] = CostCenter_Description;
Learning_Area = " ";
AOFP_APF['Learning Area'] = Learning_Area;
Learning_Group = " ";
AOFP_APF['Learning Group'] = Learning_Group;
Project_Name = " ";
AOFP_APF['Project Name'] = Project_Name;
WBS_Description = " ";
AOFP_APF['WBS Description'] = WBS_Description;
WBS_Code = " ";
AOFP_APF['WBS Code'] = WBS_Code;
Time_ID = " ";
AOFP_APF['Time.ID'] = Time_ID;
Assignment_Number = " ";
AOFP_APF['Assignment Number'] = Assignment_Number;
Vendor_Header = " ";
AOFP_APF['Vendor Header'] = Vendor_Header;
Vendor_Header_Description = " ";
AOFP_APF['Vendor Header Description'] = Vendor_Header_Description;
Spend_Type = " ";
AOFP_APF['Spend Type'] = Spend_Type;
Account_Groupings = " ";
AOFP_APF['Account Groupings'] = Account_Groupings;
PA_Groupings = " ";
AOFP_APF['PA_Groupings'] = PA_Groupings;
Firm_Projects_Groupings = " ";
AOFP_APF['Firm Projects Groupings'] = Firm_Projects_Groupings;
Concat = " ";
AOFP_APF['Concat'] = Concat;

# Combining AO GL Details + BPC with AO Front Page data
AO_BPC_comb_AO_APF = pd.concat([AO_BPC_comb,AOFP_APF]);

# Finally Rearranging columns for AOFP_APF to match overall structure
AO_BPC_comb_AO_APF_cols = ['Ledger Fiscal Year','Fiscal Period','GL Account','GL Account Description','CostCenter Description','CostCenter','Business Line','Learning Area','Learning Group','US/USI','Project Name','WBS Description','WBS Code','Time.ID','Assignment Number','Vendor Header','Vendor Header Description','Amount','CATEGORY','Spend Type','Account Groupings','P&A Groupings','Firm Projects Groupings','Fiscal Period Sorting','Expense Group','Expense Group Sort Order','Grouping','Data Source','Concat'];
AO_BPC_comb_AO_APF = AO_BPC_comb_AO_APF[AO_BPC_comb_AO_APF_cols];

#################***************************************************  Headcount HCRF  ********************************************############

# Reading the data
HCRF_Actuals = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\HCRF data P7.xlsb', sheet_name='HCRF Actuals', header=3, usecols=range(2,27));
HCRF_Actuals['CATEGORY'] = cat_act;

# Reading the data
HCRF_Plan = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\HCRF data P7.xlsb', sheet_name='HCRF Plan', header=4, usecols=range(2,27));
HCRF_Plan['CATEGORY'] = cat_pln;

# Reading the data
HCRF_Forecast = pd.read_excel(r'C:\Users\deebhatt\Desktop\SQL Queries BPC\Python inputs\FY24\P06\HCRF data P7.xlsb', sheet_name='HCRF Forecast', header=4, usecols=range(2,27));
HCRF_Forecast['CATEGORY'] = cat_fct;

# Renaming the columns as per requirement
HCRF_Actuals.rename(columns = {'Unnamed: 7':'Business Group', 'Profit Center':'CostCenter', 'PC US/USI':'US/USI', 'PC Business Line':'Business Line','Activities':'Movement'}, inplace = True);
HCRF_Plan.rename(columns = {'Unnamed: 7':'Business Group', 'Cost Center':'CostCenter', 'PC USI':'US/USI', 'PC Business Line':'Business Line', 'Movement|-':'Movement'}, inplace = True);
HCRF_Forecast.rename(columns = {'Unnamed: 7':'Business Group', 'Cost Center':'CostCenter', 'PC USI':'US/USI', 'PC Business Line':'Business Line', 'Movement|-':'Movement'}, inplace = True);

# Filtering based on Business Group and Job Level summary
condition_dbg1 = HCRF_Actuals['Business Group'].str.contains('DEVELOPMENT ADVISORS');
condition_dbg2 = HCRF_Actuals['Business Group'].str.contains('US GLOBAL');
condition_dbg3 = HCRF_Actuals['Business Group'].str.contains('DEVELOPMENT LEADERSH');
condition_dbg4 = HCRF_Actuals['Business Group'].str.contains('DELOITTE UNIVERSITY');
condition_jls = HCRF_Actuals['Job Level Summary'].str.contains('Result');
comb_condition = (condition_dbg1 | condition_dbg2 | condition_dbg3 | condition_dbg4 | condition_jls);
HCRF_Actuals = HCRF_Actuals[~comb_condition];

# Concatenating all the data sources of HCRF
HCRF_APF = pd.concat([HCRF_Actuals,HCRF_Plan,HCRF_Forecast]);

# Mapping US/USI and creating Ledger Fiscal year
HCRF_APF['US/USI'] = HCRF_APF['US/USI'].map(dict_US_USI);
HCRF_APF['Ledger Fiscal Year'] = "2024";

# Rearranging columns as per structure 
HCRF_APF_cols = ['Ledger Fiscal Year','Mapping', 'Reporting Job level', 'Federal/Commercial', 'Cohort', 'CostCenter', 'Business Group', 'Job Level Summary', 'PC Additional Dimension', 'US/USI', 'Business Line', 'PC Delivery Center', 'Movement','CATEGORY',1,2,3,4,5,6,7,8,9,10,11,12,13];
HCRF_APF = HCRF_APF[HCRF_APF_cols];

# Unpivoting the data to have period in rows rather than columns
HCRF_APF = pd.melt(HCRF_APF, id_vars=['Ledger Fiscal Year','Mapping', 'Reporting Job level', 'Federal/Commercial', 'Cohort', 'CostCenter', 'Business Group', 'Job Level Summary', 'PC Additional Dimension', 'US/USI', 'Business Line', 'PC Delivery Center', 'Movement','CATEGORY'], var_name='Fiscal Period', value_name = 'Headcount');

# Creating Headcount column and then renaming to Amount to access this in the dashboard
HCRF_DS = "HCRF Headcount";
HCRF_APF['Data Source'] = HCRF_DS;
HCRF_APF.rename(columns = {'Headcount':'Amount'}, inplace = True);

# Concatenating with main data
AO_BPC_comb_AO_APF = pd.concat([AO_BPC_comb_AO_APF,HCRF_APF]);

# Quarter Mapping
AO_BPC_comb_AO_APF = AO_BPC_comb_AO_APF.astype({'Fiscal Period':'int64'});
AO_BPC_comb_AO_APF = AO_BPC_comb_AO_APF.merge(Period_CW, how='left', on='Fiscal Period');
AO_BPC_comb_AO_APF['Fiscal Period Sorting'] = AO_BPC_comb_AO_APF['Fiscal Period'];
AO_BPC_comb_AO_APF.drop(columns = ['Start Date','End Date','Current Date','Fiscal Period as Per date'], inplace = True);

# Mapping Sort Order creation
dict_HCRF = {'Beginning Headcount':'1','Campus Hire':'2','Interns In':'3','Experienced Hire':'3','Direct Admit':'3','Promotion In':'4','From Partner to Director Conversion In':'4','From Director to Partner Conversion In':'4','GDP In':'5','LOA In':'5','Furlough In':'5','Reduced Workload In':'5','JITS In':'5','Transfer In':'5','Homebase Activity (PP)':'5','Other Changes In':'5','Interns Out':'6','Promotion Out':'7','From Director to Partner Conversion Out':'7','Voluntary Resignation (PP)':'8','Voluntary Retirement (PP)':'8','Voluntary Termination (Non PPD)':'8','End of Year Partner Retirement':'8','Involuntary Resignation (PP)':'9','Involuntary Retirement (PP)':'9','Involuntary Termination (Non PPD)':'9','GDP Out':'10','LOA Out':'10','Furlough Out':'10','Reduced Workload Out':'10','JITS Out':'10','Transfer Out':'10','Sponsored School Departure':'10','Unsponsored School Departure':'10','Other Changes Out':'10','Ending Headcount':'10'};
AO_BPC_comb_AO_APF['Mapping SO'] = AO_BPC_comb_AO_APF['Mapping'].map(dict_HCRF);

# Renaming business line components as per required format in Dashboard
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – A&A","A&A", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("Audit","A&A", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("AUDIT","A&A", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("Advisory","RFA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – RFA","RFA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("CONSULTING","Consulting", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – Consulting","Consulting", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("TAX","Tax", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – Tax","Tax", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("CORE","DLHP", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors - DLHP","DLHP", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors - DL&HP","DLHP", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – DTA","DTA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – EA","EA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("L&D Advisors – Tax","Tax", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("Services","EA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("SERVICES","EA", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("FEDERAL","GPS", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("Federal","GPS", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("FAS","Unassigned", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("Industry","Unassigned", regex = True);
AO_BPC_comb_AO_APF['Business Line'] = AO_BPC_comb_AO_APF['Business Line'].replace("LeaderDev","Unassigned", regex = True);

# Business Line Sort Order Creation
dict_BLSO = {'Tax':'1','RFA':'2','Consulting':'3','A&A':'4', 'EA':'5', 'DLHP':'6', 'DTA':'7', 'L&D Leadership':'8', 'Deloitte University':'9', 'GPS':'10', 'Unassigned':'11'}
AO_BPC_comb_AO_APF['Business Line Sort Order'] = AO_BPC_comb_AO_APF['Business Line'].map(dict_BLSO);





