from model.datamodel import DepartmentModel
from datetime import datetime
from utility import logs
class AddEditDepartments():

    def placements_data_departments(data,session,process_start):
        try:        
            for index,row in data.iterrows():
                    result = session.query(DepartmentModel).filter(DepartmentModel.department_id == row['id']).first()
                    
                    if result:
                        AddEditDepartments.update_placements(result,row,session)
                    else:
                        AddEditDepartments.insert_placements(Data=row,session=session)
            logs.log(log_name='placements_data_departments',log_start= process_start, start_date=datetime.now(),session=session)
        except Exception as e:
            logs.log(log_name='placements_data_departments',log_start= process_start, start_date=datetime.now(),session=session,error=e)
    def insert_placements(Data,session):
        model = DepartmentModel(department_id=Data['id'],
                                            name=Data['name'],
                                            offerscount=Data['offers_count'],
                                            status=Data['status'])
        

        
        session.add(model)
        session.commit()
    
    def update_placements(result,Data,session):
            
            result.department_id=Data['id']
            result.name=Data['name']
            result.offerscount=Data['offers_count']
            session.commit()
            
    

    
            

    
    
    
    



