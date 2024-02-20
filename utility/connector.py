from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utility import constant 



class Server():
    
    
    def Connector(self,staging_or_production):
        driver_name = "{ODBC Driver 17 for SQL Server}"

        if staging_or_production=='production':
            server = str(constant.data_server_Production['server'])
            database = str(constant.data_server_Production['database'])
            user = str(constant.data_server_Production['user'])
            pwd = str(constant.data_server_Production['pwd'])
            connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=' + server + ';uid=' + user + ';pwd=' + pwd + ';Database=' + database
            engine = create_engine("mssql+pyodbc:///?odbc_connect=" + connection_string)
        else:
            server = str(constant.data_server_Staging['server'])
            database = str(constant.data_server_Staging['database'])
            user = str(constant.data_server_Staging['user'])
            pwd = str(constant.data_server_Staging['pwd'])

            connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=' + server + ';Database=' + database + ';Trusted_Connection=yes;'
            engine = create_engine("mssql+pyodbc:///?odbc_connect=" + connection_string)
        


        Session = sessionmaker(bind=engine)
        session = Session()
        return session