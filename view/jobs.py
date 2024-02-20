from model.datamodel import jobs_model,tags_model
from datetime import datetime
from utility import logs



class AddEditJobTag():

    def jobandtag_data_processing(data,session,process_start,filter_date):
        try:
            data = data['details']
          

            for row in data:
                result = session.query(jobs_model).filter(jobs_model.jobs_id==row['id']).first()
                row['last_activity_at'] = datetime.strptime(row['last_activity_at'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                row['created_at'] = datetime.strptime(row['created_at'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

                if result:
                    result.jobs_id = row['id']
                    result.has_avatar = str(row['has_avatar'])
                    result.photo_thumb_url = row['photo_thumb_url']
                    result.positive_ratings = row['positive_ratings']
                    result.rating_visible = str(row['rating_visible'])
                    result.sources_id =  None if len(row['sources'])==0 else row['sources'][0]['id']
                    result.sources_name =  None if len(row['sources'])==0 else row['sources'][0]['name']
                    result.last_activity_at = row['last_activity_at']
                    result.created_at = row['created_at']
                else:
                    model = jobs_model(jobs_id=row['id'],has_avatar=row['has_avatar'],initials=row['initials'],
                                       photo_thumb_url=row['photo_thumb_url'],positive_ratings=row['positive_ratings'],
                                       rating_visible=row['rating_visible'],
                                       sources_id= None if len(row['sources'])==0 else row['sources'][0]['id'],
                                       sources_name=None if len(row['sources'])==0 else  row['sources'][0]['name'],
                                       last_activity_at=row['last_activity_at'],
                                       created_at=row['created_at'])
                    session.add(model)

                session.commit()    
                AddEditJobTag.tags_data_processing(row['tags'],session)
                
                
            logs.log(log_name= 'Job',start_date=process_start,log_start= datetime.now(),session=session)
        except Exception as e:
            logs.log(log_name= 'Job',start_date=process_start,log_start= datetime.now(),session=session,error=e)
    
        return data

    def tags_data_processing(data,session):

        for sublist in data:
            if sublist:
                result  = session.query(tags_model).filter(tags_model.tags_id==sublist['id']).first()

                if result:
                    result.tags_id = sublist['id']
                    result.name = sublist['name']
                else:
                    model = tags_model(tags_id = sublist['id'],
                                       name = sublist['name'])
                    session.add(model)
                
                session.commit()

            
                




