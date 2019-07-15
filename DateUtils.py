import datetime,time
import requests



class DateUtils(object):

    @classmethod
    def formatTimeStamp(self,timestamp):

        tmpTimestamp=0
        if len(str(timestamp))==10:
            tmpTimestamp=int(timestamp)
            dateArray = time.localtime(timestamp)
        elif len(str(timestamp))==13:
            tmpTimestamp = timestamp/1000
        timeArray = time.localtime(tmpTimestamp)
        return time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    def get_date_offset(self,type, offset, pattern):
        today = datetime.datetime.now()
        offset_time = datetime.timedelta(hours=0)
        if type == 'hour':
            offset_time = datetime.timedelta(hours=offset)
        elif type == "day":
            offset_time = datetime.timedelta(days=offset)
        elif type=="month":
            result_date = today + offset_time
            return result_date.strftime(pattern)
        result_date = today + offset_time
        # "%Y-%m-%d %H:%M:%S"
        return result_date.strftime(pattern)

