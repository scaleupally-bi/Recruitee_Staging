from model.datamodel import CadidateModel,activities_model
import numpy as np
from datetime import datetime
from utility import logs
from utility.request import Api
import pandas as pd
import re
import math



class AddEditCandidate():
    global country
    global seniority
    global positive_ratings
    global session
    
    def candidate_data_processing(data,session,process_start,qualified_or_disqualified,two_days_ago_formatted):
        try:

            Filtered_data = data[data['updated_at']>=two_days_ago_formatted]
            candidate  = Filtered_data[['id','name','emails','phones','example','followed','has_avatar',
                            'pending_result_request','photo_thumb_url','positive_ratings','rating_visible',
                            'ratings_count','referrer','source','tasks_count','unread_notifications',
                            'upcoming_event','created_at','updated_at','viewed']]

            candidate.replace({np.nan: None}, inplace=True)
            for index, row in candidate.iterrows():
                


                result = session.query(CadidateModel).filter(CadidateModel.candidate_id == row['id']).first()
                
                if qualified_or_disqualified=='deleted':
                    if result:
                        result.deleted ='True'
                        session.commit()
                    
                else:
                  
                    if result:
                       
                        pass
                        # AddEditCandidate.update_candidate(result,row,session,qualified_or_disqualified)
                    else:
                        dt=  row['updated_at']
                        formated_date  = dt.strftime('%Y-%m-%d %H:%M:%S')
                        row['updated_at']=formated_date
    
                        dt=  row['created_at']
                        formated_date  = dt.strftime('%Y-%m-%d %H:%M:%S')
                        row['created_at']=formated_date
    
                        responce = Api.api_responce('/candidates/' + str(row['id'])).json()
                        details_candidate = responce['candidate']
                        pattern  = re.compile('Country*', re.IGNORECASE)
                    
                        if pattern.search(str(details_candidate['tags'])):
                            AddEditCandidate.country= details_candidate['tags'][0][9:] 
                        else:
                            AddEditCandidate.country = None
                        sources_d =  responce['sources']
                        for lst_item in details_candidate['fields']:
                            if 'name' in lst_item:
                                if lst_item['name']=='Seniority':
                                    AddEditCandidate.seniority = None if len(lst_item['values'])==0 else str(lst_item['values'][0]['value'])
                              
                    
                        AddEditCandidate.positive_ratings= details_candidate['positive_ratings']
                        AddEditCandidate.insert_candidate(row=row,session=session,qualified_or_disqualified=qualified_or_disqualified)

                        if qualified_or_disqualified!='deleted':
                            for index, row in candidate.iterrows():
                                AddEditCandidate.activity(row['id'],session,process_start)

            logs.log(log_name=qualified_or_disqualified + ' candidate data processing',log_start= process_start, start_date=datetime.now(),session=session)
        except Exception as e:
             logs.log(log_name= qualified_or_disqualified + ' candidate data processing',log_start= process_start, start_date=datetime.now(),session=session,error=e)
    def insert_candidate(row,session,qualified_or_disqualified):
        if qualified_or_disqualified=="qualified":
            Cadidate_Model = CadidateModel(candidate_id=row['id'],name=row['name'],emails=str(row['emails']),phones=str(row['phones']),example=str(row['example']),
                                        followed=str(row['followed']),has_avatar=str(row['has_avatar']),pending_result_request=str(row['pending_result_request']),
                                        photo_thumb_url=row['photo_thumb_url'],positive_ratings=row['positive_ratings'],rating_visible=str(row['rating_visible']),
                                        ratings_count=row['ratings_count'],referrer=row['referrer'],source=row['source'],
                                        sources=row['sources'],tasks_count=row['tasks_count'],unread_notifications=str(row['unread_notifications']),upcoming_event=str(row['upcoming_event']),created_at=row['created_at'],updated_at=row['updated_at'] ,
                                        viewed=str(row['viewed']),qualified='True',country= AddEditCandidate.country,
                                        seniority = AddEditCandidate.seniority)
        elif qualified_or_disqualified=="disqualified":
             Cadidate_Model = CadidateModel(candidate_id=row['id'],name=row['name'],emails=str(row['emails']),phones=str(row['phones']),example=str(row['example']),
                                        followed=str(row['followed']),has_avatar=str(row['has_avatar']),pending_result_request=str(row['pending_result_request']),
                                        photo_thumb_url=row['photo_thumb_url'],positive_ratings=row['positive_ratings'],rating_visible=str(row['rating_visible']),
                                        ratings_count=row['ratings_count'],referrer=row['referrer'],
                                        sources=row['sources'],source=row['source'],tasks_count=row['tasks_count'],
                                        unread_notifications=str(row['unread_notifications']),upcoming_event=str(row['upcoming_event']),created_at=row['created_at'],updated_at=row['updated_at'] ,
                                        viewed=str(row['viewed']),disqualified='True',country= AddEditCandidate.country,
                                        seniority = AddEditCandidate.seniority)
        else:
            CadidateModel(candidate_id=row['id'],name=row['name'],emails=str(row['emails']),phones=str(row['phones']),example=str(row['example']),
                                        followed=str(row['followed']),has_avatar=str(row['has_avatar']),pending_result_request=str(row['pending_result_request']),
                                        photo_thumb_url=row['photo_thumb_url'],positive_ratings=row['positive_ratings'],rating_visible=str(row['rating_visible']),
                                        ratings_count=row['ratings_count'],referrer=row['referrer'],source=row['source'],sources=row['sources'],tasks_count=row['tasks_count'],
                                        unread_notifications=str(row['unread_notifications']),upcoming_event=str(row['upcoming_event']),created_at=row['created_at'],updated_at=row['updated_at'] ,
                                        viewed=str(row['viewed']),country= AddEditCandidate.country,
                                        seniority = AddEditCandidate.seniority)
                
        session.add(Cadidate_Model)
        session.commit()
    def update_candidate(result,row,session,qualified_or_disqualified):
            result.candidate_id=row['id']
            result.name=row['name']
            result.emails=str(row['emails'])
            result.phones=str(row['phones'])
            result.example=str(row['example'])
            result.followed=str(row['followed'])
            result.has_avatar=str(row['has_avatar'])
            result.pending_result_request=str(row['pending_result_request'])
            result.photo_thumb_url=row['photo_thumb_url']
            result.positive_ratings=row['positive_ratings']
            result.rating_visible=str(row['rating_visible'])
            result.ratings_count=row['ratings_count']
            result.referrer=row['referrer']
            result.source=row['source']
            result.sources=row['sources']
            result.tasks_count=row['tasks_count']
            result.unread_notifications=str(row['unread_notifications'])
            result.upcoming_event=str(row['upcoming_event'])
            result.created_at=row['created_at']
            result.updated_at=row['updated_at']
            result.viewed=str(row['viewed'])
            result.country= AddEditCandidate.country
            result.seniority = AddEditCandidate.seniority
            if qualified_or_disqualified=='qualified':
                 result.qualified ='True'
            elif qualified_or_disqualified=='disqualified':
                 result.disqualified ='True'
            
            session.commit()
    

    def activity(candidate_id,session,process_start):
        try:
            responce_activity = Api.api_responce('/tracking/candidates/' +  str(candidate_id) + '/activities').json()
            activities = Api.json_to_dataframe(responce_activity,'activities')
            columns  = ['activity_id','candidate_id','offer_id','initials','event','message_html','created_at']
            activity_data = pd.DataFrame()
            for row in activities.iterrows():
                result = session.query(activities_model).filter(activities_model.activity_id == row[1]['id']).first()
                
                if  result:
                    pass
                else:
                    model = activities_model(activity_id = row[1]['id'],
                                            candidate_id =row[1]['candidate']['id'],
                                            offer_id= None if isinstance(row[1]['offer'], dict) else None if math.isnan(row[1]['offer']) else row[1]['offer']['id'],
                                            initials=row[1]['initials'],
                                            event=row[1]['event'],
                                             message_html=row[1]['message_html'],
                                            created_at=datetime.strptime(row[1]['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S'))
                
                    session.add(model)
                    session.commit()
                
                
        except Exception as e:
                logs.log(log_name='Activity Data Processing',log_start= process_start, start_date=datetime.now(),session=session,error=e)
