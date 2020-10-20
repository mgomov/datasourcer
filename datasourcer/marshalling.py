import copy
import json
import logging
import pprint
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import datasourcer.datasourcer as dscer
from datasourcer.datasourcer import (CreateType, Dataset, Datasource,
                                     Directory, DirectoryType, File,
                                     FileFormat, RemoteDirectory, RetrieveType)


def parse_datasource_spec(
    ds_spec: dict, parent: Optional[Datasource]
) -> Optional[Datasource]:
    # ds_copy = Datasource(**copy.deepcopy(ds_spec))
    ds_copy = copy.deepcopy(ds_spec)

    try:
        ds_obj = dscer.Datasource(**ds_copy, parent=parent)
    except TypeError as e:
        logging.error(
            "Malformed datasource spec: {}\nObject: {}".format(
                e, pprint.pformat(ds_spec, depth=1, indent=4)
            )
        )

        return None

    datasets = {}
    for subset_name, subset_spec in ds_copy["datasets"].items():
        # ds_copy["datasets"][subset_name] = parse_dataset_spec(subset_spec)
        datasets[subset_name] = parse_dataset_spec(subset_spec, ds_obj)

    ds_obj.datasets = datasets

    datasources = {}
    for subsource_name, subsource_spec in ds_copy["datasources"].items():
        # ds_copy["datasources"][subsource_name] = parse_datasource_spec(subsource_spec)
        datasources[subsource_name] = parse_datasource_spec(subsource_spec, ds_obj)

        ds_obj.datasources = datasources

    return ds_obj


def parse_dataset_spec(ds_spec: dict, parent: Datasource):
    ds_copy = copy.deepcopy(ds_spec)

    try:
        ds_obj = Dataset(**ds_copy, parent=parent)
    except TypeError as e:
        logging.error(
            "Malformed dataset spec: {}\nObject: {}".format(
                e, pprint.pformat(ds_spec, depth=1, indent=4)
            )
        )

        return None

    # ds_copy["org"] = parse_dir_spec(ds_copy["org"])
    ds_obj.org = parse_dir_spec(ds_copy["org"], ds_obj)

    return ds_obj


def parse_dir_spec(
    dir_spec: dict, parent: Union[Dataset, Directory]
) -> Union[None, Directory, RemoteDirectory]:
    ds_copy = copy.deepcopy(dir_spec)

    dir_obj: Union[None, Directory, RemoteDirectory] = None

    try:
        ds_type = DirectoryType(ds_copy["type"])
    except ValueError as e:
        logging.error(
            f"Directory type not specified or invalid (type string: '{ds_copy['type']}')"
        )
        return None
    except KeyError as e:
        bp()

    # if set(ds_copy.keys()) == set(Directory.__annotations__):
    if ds_type == DirectoryType.LOCAL:

        try:
            dir_obj = Directory(**ds_copy, parent=parent)
        except TypeError as e:
            logging.error(
                "Malformed dir spec: {}\nObject: {}".format(
                    e, pprint.pformat(dir_spec, depth=1, indent=4)
                )
            )

            return None

        try:
            # ds_copy["create_type"] = CreateType(ds_copy["create_type"])
            ds_obj.create_type = CreateType(ds_copy["create_type"])
        except ValueError as e:
            print(e)

        dirs = {}
        for subdir_name, subdir in ds_copy["dirs"].items():
            # ds_copy["dirs"][subdir_name] = parse_dir_spec(subdir)
            dirs[subdir_name] = parse_dir_spec(subdir, dir_obj)

        ds_obj.dirs = dirs

        files = {}
        for subfile_name, subfile in ds_copy["files"].items():
            # ds_copy["files"][subfile_name] = parse_file_spec(subfile)
            files[subfile_name] = parse_file_spec(subfile, dir_obj)

        ds_obj.files = files

        return dir_obj

    # elif set(ds_copy.keys()) == set(RemoteDirectory.__annotations__):
    if ds_type == DirectoryType.REMOTE:

        try:
            # ds_copy['create_type']= CreateType(ds_copy['create_type'])
            ds_copy["retrieve_type"] = RetrieveType(ds_copy["retrieve_type"])
        except ValueError as e:
            print(e)

        try:
            dir_obj = RemoteDirectory(**ds_copy, parent=parent)
        except TypeError as e:
            logging.error(
                "Malformed remote dir spec: {}\nObject: {}".format(
                    e, pprint.pformat(dir_spec, depth=1, indent=4)
                )
            )

            return None

        return dir_obj

    else:
        logging.error(
            "Directory spec doesn't match Directory or RemoteDirectory (keys are {})\nObject: {}".format(
                ds_copy.keys(), pprint.pformat(dir_spec, depth=1, indent=4)
            )
        )
        return None


# def download_directory(parent_dir: Path, directory: Directory, level: int = 1) -> None:

#     logging.info(
#         '{}Downloading contents of directory "{}"'.format(
#             "\t" * level, directory.path_name
#         )
#     )

#     dir_path = Path(join(parent_dir, directory.path_name))

#     # either handle as a regular directory, or as a RemoteDirectory
#     if isinstance(directory, Directory):
#     elif isinstance(directory, RemoteDirectory):
#         download_remote_directory(dir_path, directory, level=level)


def parse_file_spec(file_spec: dict, parent: Directory) -> Optional[File]:
    fs_copy = copy.deepcopy(file_spec)

    file_obj: Optional[File] = None

    try:
        file_obj = File(**fs_copy, parent=parent)
    except TypeError as e:
        logging.error(
            "Malformed file spec: {}\nObject: {}".format(
                e, pprint.pformat(file_spec, depth=1, indent=4)
            )
        )

        return None

    try:
        # fs_copy["file_type"] = FileFormat(fs_copy["file_type"])
        # fs_copy["retrieve_type"] = RetrieveType(fs_copy["retrieve_type"])
        # fs_copy["create_type"] = CreateType(fs_copy["create_type"])
        file_obj.file_type = FileFormat(fs_copy["file_type"])
        file_obj.retrieve_type = RetrieveType(fs_copy["retrieve_type"])
        file_obj.create_type = CreateType(fs_copy["create_type"])

    except ValueError as e:
        print(e)

    if fs_copy["retrieve_type"] == RetrieveType.GET:
        url_check = urlparse(fs_copy["source"])
        if len(url_check.scheme) == 0 or len(url_check.netloc) == 0:
            logging.error(
                'Malformed file spec (source missing or invalid, in source "{}")\nObject: {}'.format(
                    fs_copy["source"], pprint.pformat(file_spec, depth=1, indent=4)
                )
            )
            return file_obj

    return file_obj


def parse_datasource_file(file_path: Path) -> Optional[Dict[Any, Optional[Datasource]]]:
    datasources = {}

    if exists(file_path) and isfile(file_path):
        with open(file_path) as ds_file:
            try:
                data_json = json.load(ds_file)
            except json.JSONDecodeError as e:
                logging.error(
                    f"Provided datasource file ({file_path}) is not a JSON file, parse error follows: \n{e}"
                )

                return None

            for name, ds_json in data_json.items():
                ds_spec = parse_datasource_spec(ds_json, None)
                datasources[name] = ds_spec

    else:

        logging.error(
            f'Provided datasource file path does not exist or is not a file: "{file_path}"'
        )
        return None

    return datasources
