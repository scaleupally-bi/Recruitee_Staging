from utility import request
from view.candidate import AddEditCandidate
from view.placement import AddEditPlacements
from view.department import AddEditDepartments
from view.visits import AddEditVists
from view.offers import AddEditOffer
from view.jobs import AddEditJobTag
from utility.connector import Server
import pandas as pd
from datetime import datetime, timedelta, timezone
from utility import logs
from datetime import datetime

# NEW FEATURE

try:
    process_start = datetime.now()    
    pd_id =[]
    session = Server.Connector(self=None,staging_or_production='production')
    logs.log(log_name='Process Start',log_start= process_start, start_date=datetime.now(),session=session)
    today_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    two_days_ago_utc = today_utc - timedelta(days=5000)
    two_days_ago_formatted = two_days_ago_utc.strftime('%Y-%m-%d %H:%M:%S%z')

    lst = ['disqualified','deleted','qualified']
    
    for lst_val in lst:
        Candidateresponce =request.Api.api_responce ('/candidates?' + lst_val + '=true').json()
        Data = request.Api.json_to_dataframe(Candidateresponce,'candidates')
        # Data = pd.merge(Data,pd_id,on='id',how='inner')
        candidate  = Data[['id','name','emails','phones','example','followed','has_avatar',
                'pending_result_request','photo_thumb_url','positive_ratings','rating_visible',
                'ratings_count','referrer','source','tasks_count','unread_notifications',
                'upcoming_event','created_at','updated_at','viewed']]
        
        placment_data = Data['placements']

        candidate['updated_at'] = pd.to_datetime(candidate['updated_at'], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        candidate['created_at'] = pd.to_datetime(candidate['created_at'], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
      
        AddEditCandidate.candidate_data_processing(data=candidate,session=session,process_start=process_start,qualified_or_disqualified=lst_val,two_days_ago_formatted=two_days_ago_formatted)
        AddEditPlacements.placements_data_processing(data=Data,session=session,filter_date= two_days_ago_formatted,process_start=process_start)
        
        if lst_val!='deleted':
            pass

            

    offers_responce =request.Api.api_responce ('/offers').json()
    departmentsresponce =request.Api.api_responce ('/departments').json()  
    hiresresponce =request.Api.api_responce ('/report/detail?&metric=hires').json()
    AddEditVists.visit_data_processing(session=session,filter_date=two_days_ago_formatted)
    
    departmentsData = request.Api.json_to_dataframe(departmentsresponce,'departments')
    offersData = request.Api.json_to_dataframe(offers_responce,'offers')
    offersData = offersData[offersData['updated_at']>=two_days_ago_formatted]
    

    AddEditDepartments.placements_data_departments(data=departmentsData,session=session,process_start=process_start)
    AddEditOffer.offer_data_processing(data=offersData,session=session,filter_date=two_days_ago_formatted,process_start=process_start)
    AddEditJobTag.jobandtag_data_processing(data=hiresresponce,
                                            session=session,
                                            filter_date=two_days_ago_formatted,
                                            process_start=process_start)
    
    

except Exception  as e:
    logs.log(log_name='Error',log_start= process_start, start_date=datetime.now(),session=session,error=e)



