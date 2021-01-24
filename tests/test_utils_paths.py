"""
Tests for paths to ensure the split and join methods
of paths return the expected values for various
stimulus.
"""
import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.utils import paths
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_blob_paths_split_filename():

    name, ext = paths.split_filename("one_extension.ext")
    assert name == 'one_extension', f"{name} {ext}"
    assert ext == '.ext', f"{name} {ext}"

    name, ext = paths.split_filename("two_extension.ext.zip")
    assert name == 'two_extension.ext', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = paths.split_filename("double_dot..zip")
    assert name == 'double_dot.', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = paths.split_filename("no_ext")
    assert name == 'no_ext', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"

    name, ext = paths.split_filename(".all_ext")
    assert name == '.all_ext', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"

    name, ext = paths.split_filename(".dot_start.zip")
    assert name == '.dot_start', f"{name} {ext}"
    assert ext == '.zip', f"{name} {ext}"

    name, ext = paths.split_filename("")  # empty
    assert len(name) == 0
    assert len(ext) == 0

    name, ext = paths.split_filename("with/path/file.ext") 
    assert name == 'with/path/file', f"{name} {ext}"
    assert ext == '.ext', f"{name} {ext}"

    name, ext = paths.split_filename("with/dot.in/path") 
    assert name == 'with/dot.in/path', f"{name} {ext}"
    assert ext == '', f"{name} {ext}"


def test_blob_paths_get_paths():

    bucket, path, name, ext = paths.get_parts("bucket/parent_folder/sub_folder/filename.ext")

    assert bucket == 'bucket'
    assert name == 'filename'
    assert ext == '.ext'
    assert path == 'parent_folder/sub_folder/'


def test_blob_paths_builder():

    # without trailing /, the / should be added
    template = '%datefolders/%Y/%date/'
    path = paths.build_path(template, datetime.datetime(2000, 9, 19, 1, 36, 42, 365))
    assert path == "year_2000/month_09/day_19/2000/2000-09-19/", path

    # with trailing /, the / should be retained
    template = '%datefolders/%Y/%date/'
    path = paths.build_path(template, datetime.datetime(2000, 9, 19, 1, 36, 42, 365))
    assert path == "year_2000/month_09/day_19/2000/2000-09-19/", path


if __name__ == "__main__":
    test_blob_paths_split_filename()
    test_blob_paths_get_paths()
    test_blob_paths_builder()

    print('okay')
