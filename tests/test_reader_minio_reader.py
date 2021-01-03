"""
Test the file reader
"""
import datetime
import time
import os
import json
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers import MinioReader, Reader
from gva.data.formats import dictset
from gva.flows.operators import SaveToMinioOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def epoch_to_date(timestamp):
    import datetime
    i = int(timestamp)
    while i > 2147483647:
        i = i / 1000
    return datetime.datetime.fromtimestamp(i, datetime.timezone.utc)


def group_by_day(dictset, field_name):
    ### min to max dates, filling with zeroes

    def date_it(s):
        if isinstance(s, (datetime.date, datetime.datetime)):
            return s
        if isinstance(s, str):
            try:
                s = int(s)
            except:
                return datetime.datetime.fromisoformat(s)
        i = int(s)
        while i > 2147483647:
            i = i / 1000
        return datetime.datetime.fromtimestamp(i, datetime.timezone.utc)

    groups = {}
    for item in dictset:
        dt = date_it(item.get(field_name))
        day = dt.strftime('%Y-%m-%d %H')
        groups[day] = groups.get(day, 0) + 1
    return groups


def bar_chart(data, width=40):

    maximum = 0
    for k,v in data.items():
        if v > maximum:
            maximum = v

    for k,v in data.items():
        if v == maximum:
            print(k, '*'*int(((v/maximum)*width)//1), maximum)
        else:
            print(k, '*'*int(((v/maximum)*width)//1))


if __name__ == "__main__":

    reader = Reader(
        thread_count=0,
        #select=['username'],
        #from_path='TWITTER/tweets/%datefolders/',
        from_path='TWITTER/tweets/year_%Y/month_%m/day_%d/',
        #where=lambda r: r['username'] in ['realDonaldTrump', 'BillGates', 'Twitter', 'Amazon', 'NBCNews', 'BBCNews', 'CNNNews'],
        #where=lambda r: ('coronavirus' in r['tweet'].lower()) or ('corona virus' in r['tweet'].lower()) or ('corona-virus' in r['tweet'].lower()),
        #where=lambda r: ('joyce' in r['text'].lower()),
        reader=MinioReader,
        end_point=os.getenv('MINIO_END_POINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        start_date=datetime.date(2021,1,2),
        end_date=datetime.date(2021,1,2),
        secure=False
    )


#    save = SaveToMinioOperator(
#            end_point=os.getenv('MINIO_END_POINT'),
#            access_key=os.getenv('MINIO_ACCESS_KEY'),
#            secret_key=os.getenv('MINIO_SECRET_KEY'),
#            to_path="TWITTER/tweets/%datefolders/reformatted_twitter_%date.jsonl",
#            secure=False,
#            compress=False)

    
#    reader = dictset.set_column(reader, 'timestamp', setter=lambda t: epoch_to_date(t['timestamp']).isoformat())
    
    d = group_by_day(reader, 'timestamp')
    bar_chart(d, 100)

#    for row in reader:
#        save.execute(data=row, context={})