from model.datamodel import offersModel,followersModel,pipelinetemplatemodel,stage_model
import numpy as np
from datetime import datetime
import pandas as pd
from utility import logs
from sqlalchemy import and_

class AddEditOffer():

    def offer_data_processing(data,session,filter_date,process_start):
        try:
            offerData=AddEditOffer.data_cleaning(data)
            for index,row in offerData.iterrows():
                result = session.query(offersModel).filter(offersModel.offer_id == int(row['id'])).first()
                if row['id']==1561663:
                    pass

                AddEditOffer.followers(row['followers'],row['id'],session)
                if result:
                    AddEditOffer.update_placements(result,row,session)
                else:
                    AddEditOffer.insert_placements(data=row,session=session)
                
                if row['pipeline_template']!=None:
                    AddEditOffer.pipeline_template(row=row['pipeline_template'],offier_id=row['id'],session=session)

                
            logs.log(log_name='offer_data_processing',log_start= process_start, start_date=datetime.now(),session=session)
        except Exception as e:
            logs.log(log_name='offer_data_processing',log_start= process_start, start_date=datetime.now(),session=session,error=e)
                
    def insert_placements(data,session):
        Model = offersModel(offer_id=data['id'],enabled_for_referrals=str(data['enabled_for_referrals']),
                            guid=data['guid'],example=str(data['example']),pipeline=str(data['pipeline']),city=data['city'],
                            issues_is_required_data_missing=str(data['issues']['is_required_data_missing']),
                            issues_is_requisition_missing=str(data['issues']['is_requisition_missing']),
                            status=data['status'],job_scheduler=data['job_scheduler'],remote=str(data['remote']),
                            shared_openings_count=data['shared_openings_count'],hiring_manager_id=data['hiring_manager_id'],
                            title=data['title'],careers_url=data['careers_url'],recruiter_id=data['recruiter_id'],
                            followed=str(data['followed']),offer_tags=str.replace(str(data['offer_tags']),"'",""),description=data['description'],
                            slug=data['slug'],employment_type=data['employment_type'],location=data['location'],
                            enabled_languages_code=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['code'],
                            enabled_languages_name=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['name'],
                            enabled_languages_native_name=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['native_name'],
                            kind=data['kind'],
                            eeo_settings=data['eeo_settings'],requirements=data['requirements'],position=data['position'],
                            mailbox_email=data['mailbox_email'],
                            requisitions_id=None if len(data['requisitions'])==0 else data['requisitions'][0]['id'],
                            requisitions_status=None if len(data['requisitions'])==0 else data['requisitions'][0]['status'],
                            requisitions_title=None if len(data['requisitions'])==0 else data['requisitions'][0]['title'],
                            state_name=data['state_name'],
                            disqualified_candidates_count=data['disqualified_candidates_count'],
                            pipeline_template_id=data['pipeline_template_id'],number_of_openings=data['number_of_openings'],
                            closed_at=data['closed_at'],country_code=data['country_code'],
                            candidates_count=data['candidates_count'],department_id=data['department_id'],
                            url=data['url'],postal_code=data['postal_code'],department=data['department'],
                            street=data['street'],has_active_campaign=str(data['has_active_campaign']),
                            adminapp_url=data['adminapp_url'],hired_candidates_without_openings_count=data['hired_candidates_without_openings_count'],
                            lang_code=data['lang_code'],state_code=str(data['state_code']),hired_candidates_count=data['hired_candidates_count'],
                            published_at=data['published_at'],created_at=data['created_at'],updated_at=data['updated_at']
        )

        session.add(Model)
        session.commit()
         
    def update_placements(result,data,session):
            
        result.offer_id=data['id']
        result.enabled_for_referrals=str(data['enabled_for_referrals'])
        result.guid=data['guid']
        result.example=str(data['example'])
        result.pipeline=str(data['pipeline'])
        result.city=data['city']
        result.issues_is_required_data_missing=str(data['issues']['is_required_data_missing'])
        result.issues_is_requisition_missing=str(data['issues']['is_requisition_missing'])
        result.status=data['status']
        result.job_scheduler=data['job_scheduler']
        result.remote=str(data['remote'])
        result.shared_openings_count=data['shared_openings_count']
        result.hiring_manager_id=data['hiring_manager_id']
        result.title=data['title']
        result.careers_url=data['careers_url']
        result.recruiter_id=data['recruiter_id']
        result.followed=str(data['followed'])
        result.offer_tags=str.replace(str(data['offer_tags']),"'","")
        result.description=data['description']
        result.slug=data['slug']
        result.employment_type=data['employment_type']
        result.location=data['location']
        result.enabled_languages_code=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['code']
        result.enabled_languages_name=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['name']
        result.enabled_languages_native_name=None if len(data['enabled_languages'])==0 else data['enabled_languages'][0]['native_name']
        result.kind=data['kind']
        result.eeo_settings=data['eeo_settings']
        result.requirements=data['requirements']
        result.position=data['position']
        result.mailbox_email=data['mailbox_email']
        result.requisitions_id=None if len(data['requisitions'])==0 else data['requisitions'][0]['id']
        result.requisitions_status=None if len(data['requisitions'])==0 else data['requisitions'][0]['status']
        result.requisitions_title=None if len(data['requisitions'])==0 else data['requisitions'][0]['title']
        result.state_name=data['state_name']
        result.disqualified_candidates_count=data['disqualified_candidates_count']
        result.pipeline_template_id=data['pipeline_template_id']
        result.number_of_openings=data['number_of_openings']
        result.closed_at=data['closed_at']
        result.country_code=data['country_code']
        result.candidates_count=data['candidates_count']
        result.department_id=data['department_id']
        result.url=data['url']
        result.postal_code=data['postal_code']
        result.department=data['department']
        result.street=data['street']
        result.has_active_campaign=str(data['has_active_campaign'])
        result.adminapp_url=data['adminapp_url']
        result.hired_candidates_without_openings_count=data['hired_candidates_without_openings_count']
        result.lang_code=data['lang_code']
        result.state_code=str(data['state_code'])
        result.hired_candidates_count=data['hired_candidates_count']
        result.published_at=data['published_at']
        result.created_at=data['created_at']
        result.updated_at=data['updated_at']
 
    def data_cleaning(Data):
             
        Data['created_at'] = pd.to_datetime(Data['created_at'],format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        Data['updated_at'] = pd.to_datetime(Data['created_at'],format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        Data['closed_at'] = pd.to_datetime(Data['created_at'],format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        Data['published_at']= pd.to_datetime(Data['published_at'],format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)

        Data.replace({np.nan: None}, inplace=True)
        return Data   

    def followers(followersData,offer_id,session):
        if offer_id=='1561663':
            print(str(offer_id) + ':' + str(len(followersData)))

        print(str(offer_id) + ':' + str(len(followersData)))
        for row  in followersData:
            result = session.query(followersModel).filter(and_(followersModel.followers_id==row['id'],followersModel.offer_id_fk==offer_id)).first()

            if result:
                result.followers_id= row['id']
                result.offer_id_fk=offer_id
                result.email=row['email']
                result.first_name=row['first_name']
                result.last_name=row['last_name']
                result.has_avatar=row['has_avatar']
                result.initials=row['initials']
                result.photo_normal_url=row['photo_normal_url']
                result.photo_thumb_url=row['photo_thumb_url']
                result.time_format24=row['time_format24']
                result.timezone=row['timezone']
            else:
                followers_Model = followersModel(
                                                followers_id=row['id'],
                                                offer_id_fk=offer_id,
                                                email=row['email'],
                                                first_name=row['first_name'],
                                                last_name=row['last_name'],
                                                has_avatar=row['has_avatar'],
                                                initials=row['initials'],
                                                photo_normal_url=row['photo_normal_url'],
                                                photo_thumb_url=row['photo_thumb_url'],
                                                time_format24=row['time_format24'],
                                                timezone=row['timezone'])
                session.add(followers_Model)
                
            session.commit()
    
    def pipeline_template(row,offier_id, session):
            
            result =  session.query(pipelinetemplatemodel).filter(pipelinetemplatemodel.pipeline_template_id== int(row['id'])).first()
            if result:
                
                result.pipeline_template_id=row['id']
                result.offer_id_fk=offier_id
                result.category=row['category']
                result.custom=row['custom']
                result.position=row['position']
                result.requires_adjustment=row['requires_adjustment']
                result.default=row['default']
                result.title=row['title']
            else:
                model = pipelinetemplatemodel(pipeline_template_id=row['id'],
                                              offer_id_fk=offier_id,
                                              category=row['category'],
                                            custom=row['custom'],
                                            position=row['position'],
                                            requires_adjustment=row['requires_adjustment'],
                                            default=row['default'],
                                            title = row['title'])
                session.add(model)

            session.commit()
            AddEditOffer.stage(row['stages'],row['id'],session)
                                    
    def stage(data,pipeline_id,session):

        for row in data:
            result = session.query(stage_model).filter(stage_model.stage_id==row['id']).first()

            if result:
                result.stage_id= row['id']
                result.pipeline_template_id_fk = pipeline_id
                result.action_templates = row['action_templates']
                result.category = row['category']
                result.fair_evaluations_enabled = row['fair_evaluations_enabled']
                result.group = row['group']
                result.locked = row['locked']
                result.name = row['name']
                result.placements_count = row['placements_count']
                result.position = row['position']
                result.time_limit= row['time_limit']
            else:
                model = stage_model(
                    stage_id= row['id'],
                    pipeline_template_id_fk = pipeline_id,
                    action_templates = row['action_templates'],
                    category = row['category'],
                    fair_evaluations_enabled = row['fair_evaluations_enabled'],
                    group = row['group'],
                    locked = row['locked'],
                    name = row['name'],
                    placements_count = row['placements_count'],
                    position = row['position'],
                    time_limit= row['time_limit'])
                session.add(model)
                
            
            session.commit()





                







    

    
            

    
    
    
    



