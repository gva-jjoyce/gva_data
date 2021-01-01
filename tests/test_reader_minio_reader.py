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
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def group_by_day(dictset, field_name):
    ### min to max dates, filling with zeroes
    groups = {}
    for item in dictset:
        day = item.get(field_name)
        if day is not None:
            if isinstance(day, str):
                day = day[:10]
            else:
                day = day.date()
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

    r = Reader(
        thread_count=4,
        #select=['username'],
        from_path='TWITTER/tweets/%datefolders/',
        #from_path='TWITTER/tweets/%datefolders/',
        #where=lambda r: r['username'] in ['realDonaldTrump', 'BillGates', 'Twitter', 'Amazon', 'NBCNews', 'BBCNews', 'CNNNews'],
        where=lambda r: ('coronavirus' in r['tweet'].lower()) or ('corona virus' in r['tweet'].lower()) or ('corona-virus' in r['tweet'].lower()),
        #where=lambda r: ('biden' in r['tweet'].lower()),
        reader=MinioReader,
        end_point='10.10.10.30:9000',
        access_key='57BTIM68ETSQ7ZQG',
        secret_key='LXWODW6DSZX9AD9TX9XBTW292KEOATGB',
        start_date=datetime.date(2020,1,1),
        end_date=datetime.date(2020,3,19),
        secure=False
    )

    d = group_by_day(r, 'timestamp')
    #print(d)
    bar_chart(d, 60)