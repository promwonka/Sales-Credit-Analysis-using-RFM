import os
import pandas as pd
import numpy as np
import glob

cwd = os.getcwd()

def getIndexes(dfObj, value):
    listOfPos = list()
    result = dfObj.isin([value])
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append((row, col))
    return listOfPos


def preprocess(path):
    os.chdir(path)
    extension = 'xlsx'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    list_file=[]
    for files in all_filenames:
        dataset= pd.read_excel(files,encoding = "unicode_escape")
        dataset.columns =['Date','particulars','sales', 'vch type','vch_number','debit','credit']
        listOfPositions = getIndexes(dataset, 'Ledger:')
        k=listOfPositions[0][0] 
        dataset = dataset.iloc[k:]
        dataset['customer_name']=dataset['particulars']
        dataset=dataset.replace(to_replace ="To", value =np.nan) 
        dataset=dataset.replace(to_replace ="By", value =np.nan) 
        dataset=dataset.replace(to_replace ="Particulars", value =np.nan) 
        pd.set_option('display.max_rows',None)
        pd.set_option('display.max_columns',None)
        cols = ['customer_name']
        dataset.loc[:,cols] = dataset.loc[:,cols].ffill()
        dataset = dataset[dataset.Date != 'Ledger:']
        dataset = dataset[dataset.Date !='Date']
        dataset=dataset.drop(['particulars'], axis = 1)
        dataset['credit'] = dataset['credit'].fillna(0)
        dataset['debit'] = dataset['debit'].fillna(0)
        dataset=dataset.drop(['vch type'], axis = 1) 
        dataset=dataset.drop(['vch_number'], axis = 1)
        dataset['Date'] = pd.to_datetime(dataset['Date'], errors='coerce')
        dataset=dataset.dropna(subset=['sales'])
        cols = ['Date']
        dataset.loc[:,cols] = dataset.loc[:,cols].ffill()
        dataset['sales'] = dataset['sales'].replace(['GST SALES', 'DISCOUNT ALLOWED', 'CASH','SOUTH INDIAN BANK LTD (CC A/C NO. 0038084000002275)', 'HDFC BANK'],['NP','PAYMENT','PAYMENT','PAYMENT','PAYMENT'])
        dataset.reset_index(inplace = True, drop = True)
        #dataset['Gross Total']=dataset['credit']-dataset['debit']
        #dataset=dataset.drop(['debit'], axis = 1) 
        #dataset=dataset.drop(['credit'], axis = 1)
        #dataset['days'] = dataset.groupby('customer_name')['Date'].diff().apply(lambda x: x.days)
        #dataset['balance'] = dataset.groupby('customer_name')['Gross Total'].cumsum()
        #dataset['days'] = dataset['days'].fillna(0)
        list_file.append(dataset)
    result = pd.concat(list_file)    
    result.sort_values(by=['customer_name', 'Date'])
    for value in result['sales']:
        if value == 'Opening Balance':
            result.drop_duplicates(subset=['customer_name', 'sales'])
    result = result[result.sales != 'Closing Balance']
    result.to_csv("calicut_result.csv")
    return



preprocess(cwd)

#print(preprocess())
