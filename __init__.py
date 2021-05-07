from mycroft import MycroftSkill, intent_file_handler


class SolaredgePvMonitoring(MycroftSkill):
    def __init__(self):
        MycroftSkill.__init__(self)

    @intent_file_handler('monitoring.pv.solaredge.intent')
    def handle_monitoring_pv_solaredge(self, message):
        timespan = message.data.get('timespan')
        consumption = ''
        production = ''

        self.speak_dialog('monitoring.pv.solaredge', data={
            'consumption': consumption,
            'timespan': timespan,
            'production': production
        })


def create_skill():
    return SolaredgePvMonitoring()

