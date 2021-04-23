from datetime import datetime, timedelta
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
import glob

def order_cluster(cluster_field_name, target_field_name,df_,ascending):
    new_cluster_field_name = 'new_' + cluster_field_name
    df_new = df_.groupby(cluster_field_name)[target_field_name].mean().reset_index()
    df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df_,df_new[[cluster_field_name,'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name],axis=1)
    df_final = df_final.rename(columns={"index":cluster_field_name})
    return df_final

def customer_segmentation(path):
    extension = 'xlsx'
    df_final = pd.DataFrame() 
    files = [i for i in glob.glob('*.{}'.format(extension))]
    for x in files:
        df = pd.read_excel(x, encoding='latin-1')
        df_final = pd.concat([df_final,df]) 
    print(df_final.head()) 
    df_final = df_final.dropna()
    df_final['date'] = pd.to_datetime(df_final['date'].astype(str), errors='coerce')
    df_user = pd.DataFrame(df_final['item_name'].unique())
    df_user.columns = ['item_name']
    df_max_purchase = df_final.groupby('item_name').date.max().reset_index()
    df_max_purchase.columns = ['item_name','MaxPurchaseDate']
    df_max_purchase['recency'] = (df_max_purchase['MaxPurchaseDate'].max() - df_max_purchase['MaxPurchaseDate']).dt.days
    df_user = pd.merge(df_user, df_max_purchase[['item_name','recency']], on='item_name')
    df_frequency = df_final.groupby('item_name').date.count().reset_index()
    df_frequency.columns = ['item_name','frequency']
    df_user = pd.merge(df_user, df_frequency, on='item_name')
    df_final['profit'] = (df_final['sale_price'] - df_final['cost_price'])
    df_final['profit'] = df_final['profit']*df_final['billed_quantity']
    df_profit = df_final.groupby('item_name')['profit'].sum().reset_index()
    df_user = pd.merge(df_user, df_profit, on='item_name')
    scaler = MinMaxScaler()
    df_user[['recency','frequency','profit']] = scaler.fit_transform(df_user[['recency','frequency','profit']])
    df_user = df_user.dropna()
    kmeans = KMeans(n_clusters=4)
    kmeans.fit(df_user[['recency']])
    df_user['recency_cluster'] = kmeans.predict(df_user[['recency']])
    df_user = order_cluster('recency_cluster', 'recency',df_user,False)
    kmeans = KMeans(n_clusters=4)
    kmeans.fit(df_user[['frequency']])
    df_user['frequency_cluster'] = kmeans.predict(df_user[['frequency']])
    df_user = order_cluster('frequency_cluster', 'frequency',df_user,True)
    kmeans = KMeans(n_clusters=4)
    kmeans.fit(df_user[['profit']])
    df_user['profit_cluster'] = kmeans.predict(df_user[['profit']])
    df_user = order_cluster('profit_cluster', 'profit',df_user,True)
    df_user['overall_score'] = df_user['recency_cluster'] + df_user['frequency_cluster'] + 1.33*(df_user['profit_cluster'])
    df_user['segment'] = 'Low-Value'
    df_user.loc[df_user['overall_score']>2,'segment'] = 'Mid-Value' 
    df_user.loc[df_user['overall_score']>4,'segment'] = 'High-Value' 
    df_user.to_csv('item_segmentation.csv')
    return

customer_segmentation('D:\WUDI_Internship\BusinessIntelligence\CustomerSegmentation\test2')