from setting import *

def file_mysql(path):
    user_name = "root"
    user_password = "Biadmin@123"
    database_ip = "10.0.1.73:3306"
    database_name = "vt_amz_inventory"
    database_all = "mysql+pymysql://" + user_name + ":" + user_password + "@" + database_ip + \
                   "/" + database_name + "?charset=utf8mb4"
    engine = create_engine(database_all)
    files = os.listdir(path=path)
    file_list = []
    for file in files:
        account = file.split('_')[-1].split('-')[1]
        country_export = file.split('_')[-1].split('-')[2].rstrip('.csv')
        export_time = file.split('_')[0]
        try:
            data = pd.read_csv(path + '/' + file, delimiter='\t', encoding=encod(path + '/' + file), low_memory=False,quoting=3)
        except:
            print(file, '出错了')
            break

        if data.shape[0] == 0:
            continue
        data['account'] = account
        data['country_export'] = country_export

        file_list.append(data)
    cloumn_list = ['account', 'country_export', 'country', 'snapshot_date', 'fnsku', 'sku_amazon', 'product_name', 'quantity',
                   'fulfillment_center_id', 'detailed_disposition']
    table_name = 'fba_inv_day_import'
    rename_cloumn = {'sku':'sku_amazon','fnsku':'fnsku','country':'country','snapshot-date':'snapshot_date','detailed-disposition':'detailed_disposition','product-name':'product_name','quantity':'quantity','fulfillment-center-id':'fulfillment_center_id'}

    result = pd.concat(file_list)
    result.rename(columns=rename_cloumn, inplace=True)
    data2 = result[cloumn_list]

    print(data2.columns.values)
    print('----------------------')
    print(data2)

    pd.io.sql.to_sql(data2, table_name, con=engine, if_exists='append', index=False)

