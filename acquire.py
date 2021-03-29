import env
import pandas as pd

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_connection(db, user=env.user, host=env.host, password=env.password):
    
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_mallcustomer_data():
    '''
    Grab our data from path and read as dataframe
    '''
    
    df = pd.read_sql('''
                        SELECT *
                        from customers
                        ''', get_connection('mall_customers'))
    
    return df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_zillow_data():
    '''
    Grab our data from path and read as dataframe
    '''
    
    df = pd.read_sql('''
                        SELECT prop.*, 
                               pred.logerror, 
                               pred.transactiondate, 
                               air.airconditioningdesc, 
                               arch.architecturalstyledesc, 
                               build.buildingclassdesc, 
                               heat.heatingorsystemdesc, 
                               landuse.propertylandusedesc, 
                               story.storydesc, 
                               construct.typeconstructiondesc 

                        FROM   properties_2017 prop  
                               INNER JOIN (SELECT parcelid,
                                                  logerror,
                                                  Max(transactiondate) transactiondate 
                                           FROM   predictions_2017 
                                           GROUP  BY parcelid, logerror) pred
                                       USING (parcelid) 
                               LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
                               LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
                               LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
                               LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
                               LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
                               LEFT JOIN storytype story USING (storytypeid) 
                               LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
                        WHERE  prop.latitude IS NOT NULL 
                               AND prop.longitude IS NOT NULL
                               
                               AND propertylandusetypeid between 260 AND 266
                               OR propertylandusetypeid between 273 AND 279
                               AND NOT propertylandusetypeid = 274
                               AND unitcnt = 1;

                        ''', get_connection('zillow'))
    
    
    return df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~