import json
import warnings
import pymysql
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime, date
import pandas as pd
warnings.filterwarnings("ignore")
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()

from sqlalchemy import create_engine

mon = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
         'August', 'September', 'October', 'November', 'December']
# class datetim(BaseModel):
#     start_date: str
#     end_date: str
#     month: str

host = 'mysqldb.cbnidl7thuhz.us-east-1.rds.amazonaws.com'
user = 'admin'
password = 'odunayo0412'
database = 'panalyst'
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

@app.get("/updated-data")
async def root():
    data = pd.read_sql('select * from driver',engine)
    data['timestamp'] = pd.to_datetime(data['eventtime'])
    data['eventdate'] = data['timestamp'].dt.date
    data['eventtime'] = data['timestamp'].dt.time
    data['eventday'] = data['timestamp'].dt.day_name()
    data['eventyear'] = data['timestamp'].dt.year
    data['eventmonth'] = data['timestamp'].dt.month_name()
    data['eventweek'] = data['timestamp'].dt.isocalendar().week
    data['eventdayofweek'] = data['timestamp'].dt.dayofweek
    data['eventdayofyear'] = data['timestamp'].dt.dayofyear
    data['eventquarter'] = data['timestamp'].dt.quarter
    data['eventhour'] = data['timestamp'].dt.hour

    updated_data = data[['DriverID', 'positionid', 'timestamp', 'event', 'eventdate', 'eventtime', 'eventday', 'eventyear', 'eventmonth', 'eventweek',
                         'eventdayofweek', 'eventdayofyear', 'eventquarter', 'eventhour', 'latitude', 'longitude', 'altitude', 'speed', 'course', 'accuracy', ]]
    result = updated_data.to_json(orient="records")
    return {"message": "Success","status":200,"data":result}





# @app.get("/get-driver-score/{month}")
@app.api_route('/predict', methods=['GET'])
async def root(month):
    #month = "August"
    if month in mon:
        start_date = 1
        end_date = 29
        data = pd.read_sql(f"select * from driver where eventmonth='{month}'",engine)
        blackbox = data[['DriverID', 'positionid', 'timestamp', 'event', 'eventdate', 'eventtime', 'eventday', 'eventyear', 'eventmonth', 'eventweek',
                             'eventdayofweek', 'eventdayofyear', 'eventquarter', 'eventhour', 'latitude', 'longitude', 'altitude', 'speed', 'course', 'accuracy', ]]

        if blackbox.shape[0] < 600:
            return {"message": "Not Enough Records","status":401}
        result = data.to_json(orient="records")
        return {"message": "Success","status":200,"data":result}
    return {"message": "Not a valid month","status":401}

if __name__ == '__main__':
    # app.run_server(debug=True, port=5050)
    uvicorn.run(app, debug=True)