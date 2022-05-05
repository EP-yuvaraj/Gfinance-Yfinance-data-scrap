
from multiprocessing.sharedctypes import Value
from sqlite3 import Date
from fastapi import status
from sqlalchemy import null
from app.config.dbconfig import sessionLocal,conn
# from app.config import model,app_config
import psycopg2.extras
from app.leadmgmt import consumer
from app.leadmgmt import producer
# from app.leadmgmt.geticker import add_to_mst
from fastapi import APIRouter
# from app.config import schemas
from app.config.dbconfig import engine
from json import dumps
from ipaddress import collapse_addresses
from json import loads
from datetime import datetime
import os
import json


import time

from apscheduler.schedulers.background import BackgroundScheduler

globalvar=1

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")


post_route = APIRouter()

db=sessionLocal()



# @post_route.post("/stock/kafka/glive/gadd")
def addLive():
    new_data2=producer.producergaddlive()
    # new_data2=consumer.consumergaddlive()

    return new_data2


# @post_route.post("/stock/kafka/ghistory/gadd")
def addHistory():
    start="3/3/2022"
    end="4/4/2022"
    new_data2=producer.producergaddhistory(start,end)
    # new_data2=consumer.consumergaddlive()

    return new_data2


def addLiveYahoo():
    data=producer.producerMethod(1)
    return "live done"


def addHistoryYahoo():
        var1=1
        p1="1d"
        producer.producerMethodHistory(var1,p1)
        return "Historical information Added successfully "