
from sqlalchemy import null
from app.config import model
from app.config.dbconfig import sessionLocal,conn
from kafka import KafkaConsumer
from json import loads
from datetime import datetime
import pandas as pd
from app.config.dbconfig import engine
import psycopg2.extras
from kafka.errors import KafkaError


db=sessionLocal()



def consumergaddlive():
        try:
            consumer = KafkaConsumer(
            'gtestfinance',
            bootstrap_servers=['192.168.99.1:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        except KafkaError as exc:
            print("Exception during subscribing to topics - {}".format(exc))
            return

        for message in consumer:
            time1=datetime.now()
            message=message.value
            message.pop(0)

            message=pd.DataFrame(message,columns =['ticker_id', 'current_price', 'current_open_price','current_high_price','current_low_price','current_volume_price','current_change','current_change_percentage','last_trade_date_time'])
        
            print(message)
            db_data = db.query(model.TickersCurrentData.stock_price_live_id).count()

            for i in message.index:
                date_time_obj = datetime.strptime(message['last_trade_date_time'][i], "%m/%d/%Y %H:%M:%S")
                message['last_trade_date_time'][i]=date_time_obj

            
            
            df2 = pd.DataFrame(message, columns=['tool_type','current_price','current_price_date_time','current_open_price','current_high_price','current_low_price','current_volume_price','current_change','current_change_percentage','last_trade_date_time','yahoo_recommendation','ticker_id','isin_code','bu_id','sub_bu_id','application_id','is_active','created_by','created_date','last_modified_by','last_modified_date'])
            df2['bu_id']=1
            df2['sub_bu_id']=1
            df2['application_id']=1
            df2['current_price_date_time']=time1
            df2['is_active']=True
            df2['created_by']=1
            df2['created_date']=time1
            df2['last_modified_by']=1
            df2['last_modified_date']=time1
            df2.insert(0, 'stock_price_live_id', range(db_data+1,db_data+len(df2)+1))
            df2['tool_type']=2
            df2['yahoo_recommendation']="null"
        
            print(df2)
            



            df2.to_sql('txn_stock_price_live', engine,schema=None, if_exists='append',index=False)
            print("done")

            consumer.commit()
            consumer.close()
                
        return "done"


def consumergaddhistory(symbol1):
    try:
        consumer = KafkaConsumer(
        'ghistorical1',
        bootstrap_servers=['192.168.99.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    except KafkaError as exc:
            print("Exception during subscribing to topics - {}".format(exc))
            return


    for message in consumer:
        time1=datetime.now()
        message=message.value
        message.pop(0)
        message.pop(0)

        message=pd.DataFrame(message,columns =['Date', 'Open', 'High','Low','Close','Volume'])

        list1=[]
        for i in message.index:
            date_time_obj = datetime.strptime(message['Date'][i], "%m/%d/%Y %H:%M:%S")
            message['Date'][i]=date_time_obj
            check= db.query(model.TickersHistoricalData).filter(model.TickersHistoricalData.historical_price_date_time==message['Date'][i]).filter( model.TickersHistoricalData.ticker_id==symbol1).first()
            print(check)
            print(message.index[i])
            if check is not None:
                list1.append(message.index[i])
                # df2=df2.drop(df2.index[i])
        # res = str(list1)[1:-1]
        print(list1)
        print(message)
        if (len(list1)!=0):
            df2=message.drop(list1)
        else:
            df2=pd.DataFrame(message)
        print(df2)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('select isin_code from mst_stock_detail where ticker_name=%s',[symbol1,])
        isin=cursor.fetchall()
        # isin=db.query(model.MstStockDetail).filter(model.MstStockDetail.ticker_name==symbol1).filter(model.MstStockDetail.isin_code).first()
        print(isin)
        db_data = db.query(model.TickersHistoricalData.stock_price_historical_id).count()
        
        df2 = pd.DataFrame(df2, columns=['tool_type','Date','Open','High','Low','Close','Volume','ticker_id','isin_code','bu_id','sub_bu_id','application_id','is_active','created_by','created_date','last_modified_by','last_modified_date'])
        df2.rename(columns = {'Date':'historical_price_date_time','Open' : 'day_open_price', 'High' : 'day_high_price','Low':'day_low_price' , 'Close':'day_close_price' , 'Volume':'day_volume'}, inplace = True)
        print(isin[0])
        print(isin[0][0])
        df2['bu_id']=1
        df2['sub_bu_id']=1
        df2['application_id']=1
        df2['is_active']=True
        df2['created_by']=1
        df2['created_date']=time1
        df2['last_modified_by']=1
        df2['last_modified_date']=time1
        df2.insert(0, 'stock_price_historical_id', range(db_data+1,db_data+len(df2)+1))
        df2['ticker_id']=symbol1
        df2['isin_code']=isin[0][0]
        df2['tool_type']=2
        # print(message)
        print(df2)

        df2.to_sql('txn_stock_price_historical', engine,schema=None, if_exists='append',index=False)
        print("done")
        consumer.commit(offsets=None)
        consumer.close()
        # sys.exit("done")
    return "Please Check Database"




def ConsumerHistory(symbol1):
    try:
        consumer = KafkaConsumer(
        'scrappy-yhistorical',
        bootstrap_servers=['192.168.99.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    except KafkaError as exc:
            print("Exception during subscribing to topics - {}".format(exc))
            return


    for message in consumer:
        time1=datetime.now()
        message=message.value
        message=pd.read_json(message)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('select isin_code from mst_stock_detail where ticker_name=%s',[symbol1,])
        isin=cursor.fetchall()
        # isin=db.query(model.MstStockDetail).filter(model.MstStockDetail.ticker_name==symbol1).filter(model.MstStockDetail.isin_code).first()
        print(isin)
        db_data = db.query(model.TickersHistoricalData.stock_price_historical_id).count()
        df2 = pd.DataFrame(message, columns=['Open','High','Low','Close','Volume'])
        list1=[]
        for i in range(len(df2)):
            print(df2.index[i])
            # date_time_obj = datetime.strptime(message['Date'][i], "%m/%d/%Y %H:%M:%S")
            # message['Date'][i]=date_time_obj
            check= db.query(model.TickersHistoricalData).filter(model.TickersHistoricalData.historical_price_date_time==df2.index[i]).filter( model.TickersHistoricalData.ticker_id==symbol1).first()
            print(check)
            if check is not None:
                # df3=df2.drop(df2.index[i])
                list1.append(df2.index[i])
                # df2=df2.drop(df2.index[i])
        # res = str(list1)[1:-1]
        print(list1)
        print(df2)
        if (len(list1)!=0):
            df3=df2.drop(list1)
        else:
            df3=pd.DataFrame(df2)
        print(df3)
        if(len(df3)!=0):
            df3 = pd.DataFrame(df3, columns=['tool_type','Open','High','Low','Close','Volume','ticker_id','isin_code','bu_id','sub_bu_id','application_id','is_active','created_by','created_date','last_modified_by','last_modified_date'])
            df3.rename(columns = {'Open' : 'day_open_price', 'High' : 'day_high_price','Low':'day_low_price' , 'Close':'day_close_price' , 'Volume':'day_volume'}, inplace = True)
            print(isin[0])
            print(isin[0][0])
            df3['bu_id']=1
            df3['sub_bu_id']=1
            df3['application_id']=1
            df3['is_active']=True
            df3['created_by']=1
            df3['created_date']=time1
            df3['last_modified_by']=1
            df3['last_modified_date']=time1
            df3.insert(0, 'stock_price_historical_id', range(db_data+1,db_data+len(df3)+1))
            df3['ticker_id']=symbol1
            df3['isin_code']=isin[0][0]
            df3['tool_type']=1
            print(df3)
            # print(message)
            df3.to_sql('txn_stock_price_historical', engine,schema=None, if_exists='append',index=True,index_label='historical_price_date_time')
            print("done")
        consumer.commit(offsets=None)
        consumer.close()
        # sys.exit("done")
    return "Historical information Added successfully "


def Consumer(symbol):
        try:
            consumer = KafkaConsumer(
            'scrappy-yahoolive',
            bootstrap_servers=['192.168.99.1:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        except KafkaError as exc:
            print("Exception during subscribing to topics - {}".format(exc))
            return

    
        for message in consumer:

            message=message.value
            print(message)
            # print(consumer)
            db_data = db.query(model.TickersCurrentData.stock_price_live_id).count()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute('select isin_code from mst_stock_detail where ticker_name=%s',[symbol,])
            isin=cursor.fetchall()
            # print(message[0])
            print(isin)
                            
            new_data={}
            # print(message[0])
            new_data=model.TickersCurrentData(
                ticker_id=symbol,
                current_price=message['currentPrice'],
                current_high_price=message['dayHigh'],
                current_low_price=message['dayLow'],
                yahoo_recommendation=message['recommendationKey'],
                current_volume_price=0,
                current_change=0.0,
                current_change_percentage=0.0,
                tool_type=1,
                stock_price_live_id=db_data+1,
                bu_id=1,
                sub_bu_id=1,
                application_id=1,
                is_active=True,
                created_by=1,
                last_modified_by=1,
                isin_code=isin[0][0],
                current_open_price=0.0,
            )
            db.add(new_data)
                # i=i+1

            db.commit()
            db.close()
            consumer.commit()
        # KafkaConsumer.close()
            consumer.close()
        # print('Data at {} added to POSTGRESQL'.format(new_data,collections))
                
        return new_data
