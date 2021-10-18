from mycroft import MycroftSkill, intent_file_handler
from adapt.intent import IntentBuilder
from mycroft import intent_handler
from mycroft.util.time import now_local, default_timezone

from lingua_franca.parse import extract_datetime

from mycroft.util.log import LOG

import requests
import urllib.parse
import json
from datetime import datetime, timedelta, date
from .mysql_client import mysql_client


class SolaredgePvMonitoring(MycroftSkill):
    def __init__(self):
        MycroftSkill.__init__(self)

    def initialize(self):
        self.siteID = self.settings.get('siteID', None)
        self.apiKey = self.settings.get('apiKey', None)
        if not self.siteID or not self.apiKey:
            self.speak_dialog("credentials_missing")
        self.settings_change_callback = self.backend_change

        # database stuff
        self.use_storage = self.settings.get("use_storage", False)
        self.use_ssl = self.settings.get("use_ssl", False)
        self.db_lang = self.settings.get("db_lang", None)
        self.db_name = self.settings.get("db_name", None)
        # automated check interval
        self.check_intervall = 900  # seconds

        if self.use_storage:
            if not self.db_lang or not self.db_name:
                self.speak_dialog('database_credentials_missing')
            else:
                self.db_init()
                self.schedule_repeating_event(self.handle_solardata_storage,
                                              None,
                                              self.check_intervall,
                                              name="SolarStorage")
                self.recent_checktime = now_local()
                LOG.info("Next solar data check: {}".format((now_local(
                )+timedelta(seconds=self.check_intervall)).strftime("%Y-%m-%d %H:%M:%S")))

        # SE Aggregation granularity
        self.SE_timeUnits = self.translate_namedvalues('granularity')
        for unit in self.SE_timeUnits.keys():
            self.register_vocabulary(unit, 'granularity')
        # eg power/energy/...
        self.subject = self.translate_namedvalues('subject')
        for item in self.subject.keys():
            self.register_vocabulary(item, 'subject')

    def backend_change(self):
        siteID = self.settings.get('siteID', None)
        apiKey = self.settings.get('apiKey', None)
        use_storage = self.settings.get('use_storage', False)
        use_ssl = self.settings.get('use_ssl', False)
        db_name = self.settings.get('db_name', None)
        db_lang = self.settings.get("db_lang", None)
        # voice feedbacks
        if self.siteID != siteID or self.apiKey != apiKey:
            self.speak_dialog("credentials_changed")
            self.siteID = siteID
            self.apiKey = apiKey
        if (db_name != self.db_name or db_lang != self.db_lang) and use_storage:
            self.speak_dialog("database_credentials_changed")
            self.db_name = db_name
            self.db_lang = db_lang
        # need reinit dbclient
        if use_storage != self.use_storage or use_ssl != self.use_ssl:
            db_usage = {False: "database_usage_off",
                        True: "database_usage_on"}
            ssl_usage = {False: "database_ssl_usage_off",
                         True: "database_ssl_usage_on"}
            if use_storage:
                self.speak_dialog(db_usage[use_storage])
            else:
                self.speak_dialog(ssl_usage[use_ssl])
            self.use_storage = use_storage
            self.use_ssl = use_ssl
            if (not db_name or not db_lang) and use_storage:
                self.speak_dialog('database_credentials_missing')
            elif use_storage or use_ssl:
                self.db_init()
                self.schedule_repeating_event(self.handle_solardata_storage,
                                              None,
                                              self.check_intervall,
                                              name="SolarStorage")
                self.recent_checktime = now_local()
                LOG.info("Next solar data check: {}".format((now_local(
                )+timedelta(seconds=self.check_intervall)).strftime("%Y-%m-%d %H:%M:%S")))

    def db_init(self):
        self.mysql_client = mysql_client(language=self.db_lang)
        # this case will always return False the first time since the key doesn't exist
        # this way i can ensure a one time catch of the historical data
        hist_data = self.settings.get('historical_data', False)
        self.mysql_client.set_timezone(default_timezone())

        connection = self.mysql_client.create_connection(self.db_name,
                                                         self.use_ssl)
        if connection != 0:
            self.speak_dialog('database_connection_failed',
                              data={"errormsg": connection})
        else:
            self.speak_dialog('database_connected')

            if not hist_data:
                self.speak_dialog('historical_data_load')
                try:
                    loaded = self.mysql_client.retrieve_historical_data()
                    if loaded:
                        self.speak_dialog('historical_data_success')
                        self.settings['historical_data'] = True
                except Exception as e:
                    error = str(e)
                    self.speak_dialog('historical_data_failed',
                                      data={"error": error})

    def handle_solardata_storage(self):
        # file_db = ["csv", "json", "xlsx"]
        apis_check = ["energyDetails"]  # "powerDetails",

        params = {"slice": "day",
                  "timeUnit": "QUARTER_OF_AN_HOUR",
                  "startTime": self.recent_checktime.strftime("%Y-%m-%d %H:%M:%S"),
                  "endTime": now_local().strftime("%Y-%m-%d %H:%M:%S")}

        for api in apis_check:
            data = \
                self.mysql_client.get_api_response(api,
                                                   **params)

            self.mysql_client.to_sql(data, api, checkTime=True)

        self.recent_checktime = now_local()
        LOG.info("Data dumped")
        LOG.info("Next solar data check: {}".format((now_local(
        )+timedelta(seconds=self.check_intervall)).strftime("%Y-%m-%d %H:%M:%S")))

    @intent_handler(IntentBuilder("power_currently").require("currently").one_of("consumption", "production", "from_grid").optionally("subject").build())
    def handle_power_currently(self, message):
        '''
        Handles utterances like:
            "How much power is produced(*) at the moment(*)"
            "How is the power consumption(*) in the house right now(*)"
            "How much power is drawn(*) from grid momentarily(*)"
        '''
        tasks = {"consumption": ["LOAD", "consumption_now"],
                 "production": ["PV", "production_now"],
                 "from_grid": ["GRID", "from_grid_now"]}
        for item in tasks.keys():
            if message.data.get(item, None):
                json_code, dialog = tasks[item]
                break

        json_resp = self.mysql_client.get_api_response("currentPowerFlow",
                                                       format=False)
        value = json_resp["siteCurrentPowerFlow"][json_code]["currentPower"]
        # if they sent kW
        if json_resp["siteCurrentPowerFlow"]["unit"] == "kW":
            value *= 1000
        self.speak_dialog(dialog, data={'value': value})

    @intent_handler(IntentBuilder("compare_power").require("compare")
                    .one_of("consumption",
                            "production",
                            "from_grid",
                            "selfconsumption")
                    .optionally("subject").optionally("granularity")
                    .optionally("split_connector").build())
    def handle_compare_energy(self, message):
        '''
        Usage limitation:
        The API is limited to:
        A year when using daily resolution (timeUnit=DAY)
        A month when using hourly resolution of higher (timeUnit=QUARTER_OF_AN_HOUR or timeUnit=HOUR)
        Lower resolutions (weekly, monthly, yearly) have no period limitation
        '''
        API_code = {"energy": "energyDetails", "power": "powerDetails"}
        tasks = {"consumption": ("Consumption", "compare_consumption"),
                 "production": ("Production", "compare_production"),
                 "from_grid": ("Purchased", "compare_from_grid"),
                 "selfconsumption": ("SelfConsumption", "compare_selfconsumption"),
                 }
        for item in tasks.keys():
            if message.data.get(item, None):
                meter, dialog = tasks[item]
                break

        utterance = message.data.get('utterance')
        now = now_local()

        # get granularity (defaults to DAY -on SE end- if none is given)
        granularity = self.SE_timeUnits.get(
            message.data.get("granularity", None), "DAY")
        dtmanipulation = {"DAY": ("00:00:00", "23:59:59"),
                          "MONTH": ("01 00:00:00", "28 23:59:59"),
                          "YEAR": ("01-01 00:00:00", "12-31 23:59:59")}

        # split utterance to get startTime and endTime (SE jargon)
        split_connector = message.data.get("split_connector", None)
        if split_connector:
            utt_list = utterance.split(' '+split_connector+' ')
            for id, utt in enumerate(utt_list):
                dt, remainder = extract_datetime(
                    utt, now_local(), lang=self.lang)
                utt_list[id] = [dt.strftime("%Y-%m-%d %H:%M:%S"),
                                utt.replace(remainder, '').strip()]
            # req_time = searched timeframe to mirror back in the answer
            req_time = utt_list[0][1]
            utt_list.sort()
            LOG.info(utt_list)
            startTime = utt_list[0][0]
            # has to be changed to the end of the timeframe, or the data is void
            endTime = utt_list[1][0].replace(dtmanipulation[granularity][0],
                                             dtmanipulation[granularity][1])
        else:
            # set the enddate to yesterday if only one date is given
            endTime = date.today() - timedelta(days=1)
            idDay = endTime.weekday()
            endTime = endTime.strftime("%Y-%m-%d 23:59:59")
            # derive req_time from granularity // eg YEAR = "last year ..."
            if granularity == "DAY":
                req_time = self.translate_list("weekdays")[idDay]
            else:
                req_time = self.translate("last_"+granularity.lower())

            startTime, _ = extract_datetime(utterance,
                                            now_local(),
                                            lang=self.lang)
            startTime = startTime.strftime("%Y-%m-%d %H:%M:%S")
        # looking for power or energy information
        subject = message.data.get("subject", None)
        if subject:
            subject_trans = self.subject[subject]
        else:
            subject_trans = "energy"
        API = API_code[subject_trans]
        json_resp = self.mysql_client.get_api_response(API,
                                                       meters=meter.upper(),
                                                       timeUnit=granularity,
                                                       startTime=startTime,
                                                       endTime=endTime)

        # list of time/value dicts
        values = json_resp[API]["meters"][0]["values"]
        # check for missing values or uncomplete data (no final value; eg calling for day data of today)
        # leveraging dtmanipulation dict
        for check in range(len(values)-1, 0, -1):
            # date is always present
            datum = values[check]["date"].replace(dtmanipulation[granularity][0],
                                                  dtmanipulation[granularity][1])
            if values[check].get("value", None) and \
                    datetime.strptime(datum, "%Y-%m-%d %H:%M:%S") < now:
                value2 = values[check]["value"]
                break

        value1 = values[0]["value"]
        LOG.info(value2)
        percent = (value2-value1)/value1*100
        LOG.info(percent)
        if percent > 0:
            tendency = self.translate("positive")
        else:
            tendency = self.translate("negative")
        self.speak_dialog(dialog, data={"time": req_time,
                                        "value": '{:.0f}'.format(abs(percent)),
                                        "tendency": tendency})


def create_skill():
    return SolaredgePvMonitoring()
