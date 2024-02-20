from datetime import datetime
from model.datamodel import log_model


def log(log_name, log_start,start_date,session,error=None):

    if error!=None:
        status='failed'
    else:
        status='pass'
    model = log_model(log_name=log_name,
                      log_start=log_start,
                      start_date= start_date,
                      end_date= datetime.now(),
                      status=status,
                      description=str(error)
                      )
    session.add(model)
    session.commit()
    