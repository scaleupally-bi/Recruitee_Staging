import requests
from utility.constant import global_veriables
import pandas as pd



class Api():
    baseUri = str(global_veriables['baseUri'])
    company_id = str(global_veriables['companyId'])
    api_token = str(global_veriables['token'])

    def api_responce(end_point):
        uri =Api.baseUri + 'c/' + Api.company_id + end_point
        responce = requests.get(uri,
                                headers={'Authorization': 'Bearer SGpWSXFhN290SXRsZisrYlhhbC96UT09'})
        
        return  responce

    def json_to_dataframe(json_data,filtername=None):

        if filtername!= None:
            data_frame  = pd.DataFrame(json_data[filtername])
        else:
            data_frame  = pd.DataFrame(json_data)
        return data_frame

