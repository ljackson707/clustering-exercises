import env
import pandas as pd

def get_connection(db, user=env.user, host=env.host, password=env.password):
    
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def get_mallcustomer_data():
    '''
    Grab our data from path and read as csv
    '''
    
    df = pd.read_sql('''
                        SELECT *
                        from customers
                        ''', get_connection('mall_customers'))
    
    return df
