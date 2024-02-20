from model.datamodel import PlacementModel
import numpy as np
from datetime import datetime
import pandas as pd
from utility import logs


class AddEditPlacements():

    def placements_data_processing(data,session,filter_date,process_start):
        try:
            
            for index,row in data.iterrows():
                if len(row['placements']):
                    for plac in row['placements']:
                        placements  = plac
                        placements=AddEditPlacements.data_cleaning(placements)
                        if placements['updated_at']>= filter_date:
                            result = session.query(PlacementModel).filter(PlacementModel.placement_id == int(placements['id'])).first()
                            
                            if result:
                                AddEditPlacements.update_placements(result,placements,session)
                                pass
                            else:         
                                AddEditPlacements.insert_placements(placements=placements,session=session)
            logs.log(log_name='placements_data_processing',log_start= process_start, start_date=datetime.now(),session=session)
        except Exception as e:
            logs.log(log_name='placements_data_processing',log_start= process_start, start_date=datetime.now(),session=session,error=e)
    def insert_placements(placements,session):
        Placement_Model = PlacementModel(placement_id=placements['id'],candidate_id=placements['candidate_id'],created_at=placements['created_at'],department_id=placements['department_id'],department_name=placements['department_name'],
                                    disqualify_kind=placements['disqualify_kind'],disqualify_reason=placements['disqualify_reason'],hired_at=placements['hired_at'],
                                    hired_by_id=placements['hired_by_id'],hired_in_other_placement=placements['hired_in_other_placement'],hired_in_this_placement=placements['hired_in_this_placement'],
                                    job_starts_at=placements['job_starts_at'],language_code=placements['language_code'],language_name=placements['language_name'],language_native_name=placements['language_native_name'],offer_id=placements['offer_id'],overdue_at=placements['overdue_at'],
                                    overdue_diff=placements['overdue_diff'],position=placements['position'],positive_ratings=placements['positive_ratings'] ,
                                    rating_visible=placements['rating_visible'],stage_id=placements['stage_id'],updated_at=placements['updated_at']
        )

        session.add(Placement_Model)
        session.commit()
        
    
    def update_placements(result,placements,session):
            
            result.placement_id=placements['id']
            result.candidate_id=placements['candidate_id']
            result.created_at=placements['created_at']
            result.department_id=placements['department_id']
            result.department_name=placements['department_name']
            result.disqualify_kind=placements['disqualify_kind']
            result.disqualify_reason=placements['disqualify_reason']
            result.hired_at=placements['hired_at']
            result.hired_by_id=placements['hired_by_id']
            result.hired_in_other_placement=placements['hired_in_other_placement']
            result.hired_in_this_placement=placements['hired_in_this_placement']
            result.job_starts_at=placements['job_starts_at']
            result.language_code=placements['language_code']
            result.language_name=placements['language_name']
            result.language_native_name=placements['language_native_name']
            result.offer_id=placements['offer_id']
            result.overdue_at=placements['overdue_at']
            result.overdue_diff=placements['overdue_diff']
            result.position=placements['position']
            result.positive_ratings=placements['positive_ratings']
            result.rating_visible=placements['rating_visible']
            result.ratings=placements['ratings']
            result.stage_id=placements['stage_id']
            result.updated_at=placements['updated_at']
            session.commit()
    
    def data_cleaning(placements):

        if placements['language']!=None:
           placements['language_code'] =placements['language']['code']
           placements['language_name'] =placements['language']['name']
           placements['language_native_name'] =placements['language']['native_name']
        else:
           placements['language_code'] =None
           placements['language_name'] =None
           placements['language_native_name'] =None
             

        date_columns = ['created_at', 'hired_at', 'job_starts_at', 'overdue_at', 'updated_at']
        for column in date_columns:
            placements[column] = None if placements[column] is None else datetime.strptime(placements[column], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')

        placements['language']=None
        columns_to_check =['disqualify_kind','disqualify_reason']
        if 'disqualify_kind' not in placements:
             placements['disqualify_kind']=None
        if 'disqualify_reason' not in placements:
             placements['disqualify_reason']=None
        return placements

    

    
            

    
    
    
    



