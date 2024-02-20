from model.datamodel import visit_model
import datetime
import pandas as pd
from utility.request import Api
from datetime import datetime 

class AddEditVists():

    def visit_data_processing(session,filter_date):
        
        pages =10

        for i in range(10):
            url = '/report/detail?metric=visits&page=' + str(i)
            request  = Api.api_responce(url).json()
            visit_data  = Api.json_to_dataframe(request,'details')
            
            for index,row in visit_data.iterrows():
                row['date'] = datetime.strptime(row['date'],'%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
                if row['date']>=filter_date:
                    result  = session.query(visit_model).filter(visit_model.visit_id== str(row['id'])).first()
                    if result:
                        pass
                    else:
                        v_model = visit_model(visit_id= row['id'],bounced= row['bounced'],browser= row['browser'],
                                            date= row['date'],device= row['device'],duration= row['duration'],
                                            entry_page= row['entry_page'],exit_page= row['exit_page'],operating_system= row['operating_system'],
                                            referrer_url= row['referrer_url'],source= row['source'],utm_campaign= row['utm_campaign'],
                                            utm_content= row['utm_content'],utm_medium= row['utm_medium'],utm_source= row['utm_source'],
                                            utm_term= row['utm_term'])
                        
                        session.add(v_model)
                        session.commit()



