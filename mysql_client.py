from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, DateTime, text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from copy import deepcopy
import json
import requests
import urllib.parse

from datetime import datetime, timedelta
from calendar import monthrange

from .config import \
    SQL_CREDENTIALS, \
    SQL_SSL, \
    SQL_DB_SCHEMAS, \
    SQL_SPLIT_TABLE_TIME, \
    SQL_DERIVATIVE_TABLES, \
    SE_CREDENTIALS

Base = declarative_base()


class mysql_client(object):
    #language->db_type
    def __init__(self, language):
        self.language = language
        self.database = ""
        self.db = None
        # create Table Objects (SQLAlchemy)
        # self.tables = { name: Table(name, MetaData(), *schema) for name, schema in SQL_DB_SCHEMAS.items() }
        # API-basetable-identifier
        self.api_tables = {"powerDetails": "power",
                           "energyDetails": "energy"}

        # reuires CREATE USER 'root'@'192.168.188.%' IDENTIFIED VIA mysql_native_password USING '***';
        # GRANT ALL PRIVILEGES ON *.* TO 'root'@'192.168.188.%' REQUIRE NONE WITH GRANT OPTION MAX_QUERIES_PER_HOUR 0
        # MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;

    def create_connection(self, database, use_ssl):
        dialect = {"mysql": "mysql+mysqldb"}
        # todo derivative_table => summary tables
        DB_TABLES = list(SQL_DB_SCHEMAS.keys())
        DB_TABLES.extend(SQL_DERIVATIVE_TABLES)
        self.database = database

        url = "{0}://{1}:{2}@{3}/{4}".format(dialect[self.language],
                                             SQL_CREDENTIALS["user"],
                                             SQL_CREDENTIALS["password"],
                                             SQL_CREDENTIALS["host"],
                                             database)
        if use_ssl:
            connect_args = {"ssl": {"ssl_ca": SQL_SSL["CA"],
                                    "ssl_cert": SQL_SSL["CCERT"],
                                    "ssl_key": SQL_SSL["CKEY"]}}
        else:
            connect_args = {}

        self.db = create_engine(url, connect_args=connect_args)

        try:
            if not database_exists(url):
                create_database(url)
            # prime tables
            tables_present = self.db.table_names()
            tables_missing = [item for item
                              in self.__map_table_name(DB_TABLES, datetime.now())
                              if item not in tables_present]
            if tables_missing:
                print(tables_missing)
                self.__create_table(tables_missing)

        except SQLAlchemyError as e:
            error = str(e)
            return error

        return 0

    def get_api_response(self, api, slice=False, format=True, **kwargs):

        apiKey = SE_CREDENTIALS["apiKey"]
        siteID = SE_CREDENTIALS["siteID"]
        url = "https://monitoringapi.solaredge.com/site/"+siteID+"/"+api+"?"

        # api_key= is always sent
        if not kwargs:
            kwargs = {}
        # kick None args
        else:
            kwargs = {key: value for key, value in kwargs.items()
                      if value is not None}
        kwargs['api_key'] = apiKey
        print(kwargs)

        # response = requests.get(url, params=kwargs)
        with open("/home/sweng/.mycroft/skills/SolaredgePvMonitoring/pv.json", "r") as dump:
            json_data = json.load(dump)

        # print(json.dumps(response.json(), indent=4))

        # if response.status_code != 200:
        #    return response.status_code
        if not format:
            return json_data  # response.json()
        else:
            return self.__format(json_data, api, slice)  # response.json()

    def to_sql(self, data, api, checkTime=False, summary=False):

        basetable = self.api_tables[api]
        meta = MetaData()
        meta.reflect(bind=self.db)

        if summary:
            basetable = "{}_{}".format(basetable, summary)

        for slice in data:
            if isinstance(slice[0][0], datetime):
                time = slice[0][0]
            else:
                time = None

            table = self.__map_table_name(basetable, time)
            if table not in self.db.table_names():
                self.__create_table(table)

            columns = [col.name for col in meta.tables[table].c
                       if col.name != "id"]
            self.__sql_dump_data(slice, table, columns)

            if checkTime and time.strftime("%H:%M:%S") == "00:00:00":
                # check date shift
                for summary in self.__check_date_shift(time):
                    table = "{}_{}".format(basetable, summary)
                    slice = self.from_sql(basetable, time, summary)
                    self.__sql_dump_data(slice, table, columns)

    # unklare situation mit time
    # def from_sql(self, basetable, timespan):
    def from_sql(self, basetable, time, timespan):

        time = datetime.now().replace(microsecond=0)
        table = self.__map_table_name(basetable, time)
        meta = MetaData()
        meta.reflect(bind=self.db)
        columns = [col.name for col in meta.tables[table].c
                   if col.name != "id" and col.name != "Time"]

        with self.db.connect() as connection:
            data = []
            # i do se a need for different math methods in the future
            startTime, endTime = self._get_timespan(time, timespan)
            for column in columns:
                print("SELECT SUM("+column+") FROM " + table
                      + " WHERE Time BETWEEN :startTime AND :endTime")
                str = text("SELECT SUM("+column+") FROM " + table
                           + " WHERE Time BETWEEN :startTime AND :endTime")
                # SQLAlchemy cant insert column/table as argumemt -> ProgrammingError
                result = \
                    connection.execute(str,
                                       startTime=startTime.strftime(
                                           "%Y-%m-%d %H:%M:%S"),
                                       endTime=endTime.strftime(
                                           "%Y-%m-%d %H:%M:%S")
                                       ).fetchone()
                # future (>2.0) implementation result.scalars[column].all()
                data.append(result[0])
            data.insert(0, endTime)
            print(data)

        return [data]

    def retrieve_historical_data(self):
        # get start of solar production
        jsonData = self.get_api_response("dataPeriod")
        startTime = jsonData["dataPriod"]["startDate"]
        endTime = datetime.now()

        # monthly, weekly, yearly data
        check = ["WEEK", "MONTH", "YEAR"]
        for timeframe in check:
            data = self.get_api_response("energyDetails",
                                         timeUnit=timeframe,
                                         startTime=startTime,
                                         endTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.to_sql(data, "energyDetails", summary=timeframe.lower())

        # quarterhour data is only present the last 365 days
        startTime = (endTime - timedelta(days=364)
                     ).strftime("%Y-%m-%d %H:%M:%S")
        timespan = SQL_SPLIT_TABLE_TIME.get("energy", False)
        data = self.get_api_response("energyDetails", slice=timespan,
                                     timeUnit="QUARTER_OF_AN_HOUR",
                                     startTime=startTime,
                                     endTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.to_sql(data, "energyDetails")

        # inventory information

    def _get_timespan(self, time, timespan):

        def leap(time): return 1 if ((time.year-1) % 4 == 0) else 0
        endTime = (time - timedelta(days=1)
                   ).replace(hour=23, minute=59, second=59)
        if timespan == "today":
            endTime = time.replace(hour=23, minute=59, second=59)
            timespan = timedelta(days=0)
        elif timespan == "day":
            timespan = timedelta(days=1)
        elif timespan == "week":
            timespan = timedelta(days=8)
        elif timespan == "month":
            timespan = timedelta(days=monthrange(time.year,
                                                 (time-timedelta(days=1)).month)[1])
        elif timespan == "year":
            timespan = timedelta(days=365+leap(time))

        print((time - timespan).replace(hour=0, minute=0, second=0))
        print((time - timespan).replace(hour=0).replace(minute=0).replace(second=0))
        startTime = (time - timespan).replace(hour=0, minute=0, second=0)

        return startTime, endTime

    def __sql_dump_data(self, slice, table, columns):

        with self.db.connect() as connection:
            for row in slice:
                for idx, item in enumerate(row):
                    if isinstance(item, datetime):
                        time = item.strftime("%Y-%m-%d %H:%M:%S")
                        row[idx] = "'"+item.strftime("%Y-%m-%d %H:%M:%S")+"'"
                    else:
                        row[idx] = str(item)

                print("INSERT INTO "+table
                      + " ("+','.join(columns)+") VALUES ("+','.join(row)+")")
                sql = text("INSERT INTO "+table
                           + " ("+','.join(columns)+") VALUES ("+','.join(row)+")")
                connection.execute(sql)

    #def __sql_dump_summary_data(self, basetable, time):
        ##todo column whitelist
        ##expand if one should be able to search in different tables
        #columns = [ col.name for col in self.tables[basetable].c if col.name != "id" and col.name != "Time" ]
        #checkTime = [("day", True),
                #("week", time.weekday()==0),
                #("month", time.day==1),
                #("year", time.day==1 and time.month==1)]
#
        #check = { timespan: self._get_timespan(time, timespan) for timespan, needed in checkTime if needed }
        #with self.db.connect() as connection:
            #for table, dt_list in check.items():
                #data = []
                #table_name = self.__map_table_name(basetable, time)
                ##i do se a need for different math methods in the future
                #for column in columns:
                #str = text("SELECT SUM("+column+") FROM "+table_name+
                #" WHERE Time BETWEEN :startTime AND :endTime" )
                ##SQLAlchemy cant insert column/table as argumemt -> ProgrammingError
                #result = connection.execute(str,
                #startTime = dt_list[0],
                #endTime = dt_list[1]
                #).fetchone()
                ##future (>2.0) implementation result.scalars[column].all()
                #data.append(result[0])
                #data.insert(0, dt_list[1])
                ##SQL insert
                #connetion.execute(text("INSERT INTO "+basename+"_"+table+" (Time,"+columns.join(',')+") VALUES ("+data.join(',')+")"))
                ##additional table manipulation

    def __map_table_name(self, tablenames, time):
        ''' Helper method to construct the table names.
            If configured a new table gets created in

            Input: str or list of str
            Output: str or list of str
        '''
        table_list = []
        if isinstance(tablenames, str):
            single = True
            tablenames = [tablenames]
        else:
            single = False

        for basename in tablenames:
            for table, timer in SQL_SPLIT_TABLE_TIME:
                if basename == table:
                    if timer == "DAY":
                        table_list.append("{}_{}_{}_{}".format(basename,
                                                               time.day,
                                                               time.month,
                                                               time.year))
                        break
                    elif timer == "WEEK":
                        table_list.append("{}_{}_{}".format(basename,
                                                            time.isocalendar()[
                                                                             1],
                                                            time.year))
                        break
                    elif timer == "MONTH":
                        table_list.append("{}_{}_{}".format(basename,
                                                            time.month,
                                                            time.year))
                        break
                    elif timer == "YEAR":
                        table_list.append("{}_{}".format(basename,
                                                         time.year))
                        break
            else:
                table_list.append(basename)
                # tablename=basename

        if single:
            return table_list[0]
        else:
            return table_list

    def __create_table(self, tables):
        ''' Creates new table.

            Input: str / list of str
        '''
        Base = declarative_base()
        new_tables = []

        if isinstance(tables, str):
            tables = [tables]

        for table in tables:
            basename = table.split('_')[0]
            kwargs = deepcopy(SQL_DB_SCHEMAS[basename])
            kwargs['__tablename__'] = table
            # injects args in skeleton Class
            table = type('Tables', (Base,), kwargs)

        Base.metadata.create_all(self.db)

    # implement slice="day",..
    # def __format(self, jsonObj, api, slice_daily=False):
    def __format(self, jsonObj, api, slice=False):

        data = []
        if api == "powerDetails" or api == "energyDetails":
            jdata = jsonObj[api]["meters"]
            data = [[datetime.strptime(item["date"], "%Y-%m-%d %H:%M:%S")]
                    for item in jdata[0]["values"]]
            for idx, time in enumerate(data):
                for meter in jdata:
                    data[idx].append(
                        meter['values'][idx].get('value', float(0)))

            if len(data) > 1:
                del data[-1]
            data = [data]

        elif api == "dataPeriod":
            data = [data["dataPeriod"]["startDate"],
                    data["dataPeriod"]["endDate"]]

        if slice:
            data = self._slice_data(data, slice)
        print(data)
        return data

    def _slice_data(self, data, slice):

        def check_timeframe(item, slice):
            if slice == "day":
                return item.strftime("%H:%M:%S") == "00:00:00"
            elif slice == "week":
                return item.weekday() == 0 and item.strftime("%H:%M:%S") == "00:00:00"
            elif slice == "month":
                return item.day == 1 and item.strftime("%H:%M:%S") == "00:00:00"
            elif slice == "year":
                return item.month == 1 and item.day == 1 and item.strftime("%H:%M:%S") == "00:00:00"
            else:
                return item

        x = data[-1]
        for idx, item in enumerate(x):
            if len(x) > 1 and isinstance(item[0], datetime) and \
                    check_timeframe(item[0], slice):
                data[-1] = x[:idx]
                data.append(x[idx:])
                self._slice_data(data, slice)
        else:
            print(data)
            return data

    def __check_date_shift(self, time):
        checkTime = [("day", time.strftime("%H:%M:%S") == "00:00:00"),
                     ("week", time.weekday() == 0),
                     ("month", time.day == 1),
                     ("year", time.day == 1 and time.month == 1)]
        return [timespan for timespan, needed in checkTime if needed]

    # skeleton Class for dynamic table creation
    class Tables(Base):
        __tablename__ = 'dummy'
        id = Column(Integer, primary_key=True)
