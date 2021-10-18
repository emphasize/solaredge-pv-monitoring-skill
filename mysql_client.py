from sqlalchemy import create_engine, MetaData, Column, Integer, text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from copy import deepcopy
import requests

from datetime import datetime, timedelta, timezone
from calendar import monthrange
from time import sleep

from .config import \
    SQL_CREDENTIALS, \
    SQL_SSL, \
    SQL_DB_SCHEMAS, \
    SQL_SPLIT_TABLE_TIME, \
    SQL_DERIVATIVE_TABLES, \
    SE_CREDENTIALS, \
    SE_API_TABLE, \
    SQL_TABLES_PREFIX

Base = declarative_base()


class mysql_client(object):
    # todo language->db_type
    def __init__(self, language):
        self.language = language
        self.database = ""
        self.db = None
        self.tz = timezone.utc

    # reuires CREATE USER 'user'@'x.x.x.%' IDENTIFIED VIA mysql_native_password USING '***';
    # GRANT ALL PRIVILEGES ON *.* TO 'user'@'x.x.x.%' REQUIRE NONE WITH GRANT OPTION MAX_QUERIES_PER_HOUR 0
    # MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;

    def create_connection(self, database, use_ssl):
        dialect = {"mysql": "mysql+mysqldb"}
        # todo derivative_table => summary tables
        tables = list(SQL_DB_SCHEMAS.keys())
        DB_TABLES = dict.fromkeys(tables, [datetime.now(tz=self.tz)])
        for table, references in SQL_DERIVATIVE_TABLES.items():
            DB_TABLES[table].extend(references)

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
            tables_present = set(self.db.table_names())
            tables = set([self.__map_table_name(table, reference)
                          for table, references in DB_TABLES.items()
                          for reference in references])
            tables_missing = tables.difference(tables_present)
            if tables_missing:
                self.__create_table(tables_missing)

        except SQLAlchemyError as e:
            error = str(e)
            return error

        return 0

    def set_timezone(self, timezone):
        self.tz = timezone

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

        response = requests.get(url, params=kwargs)
        json_data = response.json()

        if response.status_code != 200:
            return response.status_code
        elif not format:
            return json_data
        else:
            return self.__format(json_data, api, slice)

    def to_sql(self, data, api, checkTime=False, summary=''):

        basetable = SE_API_TABLE[api]
        meta = MetaData()
        meta.reflect(bind=self.db)

        for slice in data:
            if isinstance(slice[0][0], datetime) and not summary:
                reftime = slice[0][0]
            elif summary:
                reftime = summary
            else:
                reftime = None

            table = self.__map_table_name(basetable, reftime)
            if table not in self.db.table_names():
                self.__create_table(table)

            columns = [col.name for col in meta.tables[table].c
                       if col.name != "id"]
            self.__sql_dump_data(slice, table, columns)

            if checkTime and isinstance(reftime, datetime) \
                    and reftime.strftime("%H:%M:%S") == "00:00:00":
                # check date shift
                for timespan in self.__check_date_shift(reftime):
                    table = self.__map_table_name(basetable, timespan)
                    slice = self.from_sql(basetable, reftime, timespan)
                    self.__sql_dump_data(slice, table, columns)

    def from_sql(self, basetable, time, timespan):

        # time = datetime.now().replace(microsecond=0) if not time else time
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

        return [data]

    def retrieve_historical_data(self):
        # get start of solar production
        timespan = self.get_api_response("dataPeriod")
        startTime = "{} {}".format(timespan[0], "00:00:00")
        endTime = datetime.now(tz=self.tz)

        # monthly, weekly, yearly data
        api = "energyDetails"
        check = ["WEEK", "MONTH", "YEAR"]
        for timeframe in check:
            data = self.get_api_response(api,
                                         timeUnit=timeframe,
                                         startTime=startTime,
                                         endTime=datetime.now(tz=self.tz)
                                         .strftime("%Y-%m-%d %H:%M:%S"))
            self.to_sql(data, api, summary=timeframe.lower())

        # daily data is only present the last 365 days
        startTime = (endTime - timedelta(days=364)
                     ).strftime("%Y-%m-%d %H:%M:%S")
        check = ["DAY"]
        for timeframe in check:
            data = self.get_api_response(api,
                                         timeUnit=timeframe,
                                         startTime=startTime,
                                         endTime=datetime.now(tz=self.tz)
                                         .strftime("%Y-%m-%d %H:%M:%S"))
            self.to_sql(data, api, summary=timeframe.lower())

        # quarterhour data is only present the last month
        startTime = (endTime - timedelta(days=30)
                     ).strftime("%Y-%m-%d %H:%M:%S")
        table = SE_API_TABLE[api]
        timespan = SQL_SPLIT_TABLE_TIME.get(table, False)
        data = self.get_api_response(api, slice=timespan,
                                     timeUnit="QUARTER_OF_AN_HOUR",
                                     startTime=startTime,
                                     endTime=datetime.now(tz=self.tz)
                                     .strftime("%Y-%m-%d %H:%M:%S"))
        self.to_sql(data, api)

        # inventory information
        # TODO

        return True

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

        startTime = (time - timespan).replace(hour=0, minute=0, second=0)

        return startTime, endTime

    def __sql_dump_data(self, slice, table, columns):

        with self.db.connect() as connection:
            for row in slice:
                for idx, item in enumerate(row):
                    if isinstance(item, datetime):
                        row[idx] = "'"+item.strftime("%Y-%m-%d %H:%M:%S")+"'"
                    else:
                        row[idx] = str(item)

                sql = text("INSERT INTO "+table
                           + " ("+','.join(columns)+") VALUES ("+','.join(row)+")")
                connection.execute(sql)
                sleep(0.2)

    def __map_table_name(self, table, reference):
        ''' Helper method to construct the table names.
            If configured a new table gets created in

            Input: str or list of str
            Output: str or list of str
        '''
        # apply prefix
        prefix = SQL_TABLES_PREFIX.get(table.rsplit('_', 1)[0], None)
        if prefix:
            table = "{}_{}".format(prefix, table)

        if isinstance(reference, str):
            table = "{}_{}".format(table, reference)
        else:
            for splittable, timer in SQL_SPLIT_TABLE_TIME.items():
                if splittable == table:
                    if timer == "DAY":
                        table = "{}_{}_{}_{}".format(table,
                                                     reference.day,
                                                     reference.month,
                                                     reference.year)
                        break
                    elif timer == "WEEK":
                        table = "{}_{}_{}".format(table,
                                                  reference.isocalendar()[1],
                                                  reference.year)
                        break
                    elif timer == "MONTH":
                        table = "{}_{}_{}".format(table,
                                                  reference.month,
                                                  reference.year)
                        break
                    elif timer == "YEAR":
                        table = "{}_{}".format(table,
                                               reference.year)
                        break

        return table

    def __create_table(self, tables):
        ''' Creates new table.

            Input: str / list of str
        '''
        Base = declarative_base()

        if isinstance(tables, str):
            tables = (tables)

        for table in tables:
            # remove prefix and affix (to apply DB schema)
            prefixes = [item+"_" for item in SQL_TABLES_PREFIX.values()
                        if item+"_" in table]
            prefix = prefixes[0] if prefixes else ''
            basename = table.replace(prefix, '').rsplit('_', 1)[0]

            kwargs = deepcopy(SQL_DB_SCHEMAS[basename])
            kwargs['__tablename__'] = table
            # injects args in skeleton Class
            table = type('Tables', (Base,), kwargs)

        Base.metadata.create_all(self.db)

    def __format(self, jsonObj, api, slice=False):

        data = []
        if api == "powerDetails" or api == "energyDetails":
            jdata = jsonObj[api]["meters"]
            # sort based on DBSchema
            table = SE_API_TABLE[api]
            _schema = [item for item in
                       SQL_DB_SCHEMAS[table].keys()
                       if item not in ('id', 'Time')]
            # todo sort order key for different apis
            jdata = sorted(jdata,
                           key=lambda x: _schema.index(x['type']))
            data = [[datetime.strptime(item["date"], "%Y-%m-%d %H:%M:%S")]
                    for item in jdata[0]["values"]]
            for idx, time in enumerate(data):
                for meter in jdata:
                    data[idx].append(
                        meter['values'][idx].get('value', float(0)))

            # delete last entry since it isincomplete data
            if len(data) > 1:
                del data[-1]
            data = [data]

        elif api == "dataPeriod":
            data = [jsonObj["dataPeriod"]["startDate"],
                    jsonObj["dataPeriod"]["endDate"]]

        if slice:
            data = self._slice_data(data, slice)
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
