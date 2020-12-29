"""
Functions to help with handling file paths
"""
import datetime


def split_filename(filename: str):
    """ see test cases for all handled edge cases """
    if not filename:
        return '', ''

    ext = ''
    name = ''
    parts = filename.split('.')
    if len(parts) == 1:
        return filename, ''
    if parts[0] == '':
        parts.pop(0)
        parts[0] = '.' + parts[0]
    if len(parts) > 1:
        ext = '.' + parts.pop()
    if ext.find('/') > 0:
        ext = ext.lstrip('.')
        parts.append(ext)
        ext = ''
    name = '.'.join(parts)
    if ext == '.':
        name = ''
    return name, ext


def get_parts(path_string: str):
    if not path_string:
        raise ValueError('get_parts: path_string must have a value')

    parts = str(path_string).split('/')
    bucket = parts.pop(0)
    name, ext = split_filename(parts.pop())

    if ext.find('.') < 0 and name:
        parts.append(name)
        name = None

    path = '/'.join(parts) + '/'
    return bucket, path, name, ext


def build_path(path: str, date: datetime.date = None):

    if not path:
        raise ValueError('build_path: path must have a value')

    # process the path
    bucket, path_string, _, _ = get_parts(path)
    if path_string != '/':
        path_string = bucket + '/' + path_string

    return date_format(path_string, date)


def date_format(path_string:str, date: datetime.date = None):

    if not date:
        date = datetime.datetime.now()

    path_string = path_string.replace('%datefolders', 'year_%Y/month_%m/day_%d')
    path_string = path_string.replace('%date', '%Y-%m-%d')
    path_string = path_string.replace('%time', '%H%M%S')
    
    return date.strftime(path_string)
