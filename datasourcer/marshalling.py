import copy
import json
import logging
import pprint
from glob import glob
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

# import pyyaml
import yaml

from .datasourcer import (CreateType, DataCollection, DataContext, Dataset,
                          Datasource, DirectoryType, DynamicResource,
                          FileFormat, FileFormatProcessor,
                          FileFormatToExtensionMap, RemoteSubset, Resource,
                          RetrieveType, StaticResource, Subset)


def parse_datasource_spec(
    name: str, ds_spec: dict, parent: Optional[Datasource], data_context: DataContext
) -> Optional[Datasource]:
    # ds_copy = Datasource(**copy.deepcopy(ds_spec))
    ds_copy = copy.deepcopy(ds_spec)

    try:
        ds_obj = Datasource(
            name=name,
            path=Path(name),
            **ds_copy,
            parent=parent,
            data_context=data_context,
        )
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
        datasets[subset_name] = parse_dataset_spec(subset_name, subset_spec, ds_obj)

    ds_obj.datasets = datasets

    datasources = {}
    for subsource_name, subsource_spec in ds_copy["datasources"].items():
        # ds_copy["datasources"][subsource_name] = parse_datasource_spec(subsource_spec)
        datasources[subsource_name] = parse_datasource_spec(
            subsource_name, subsource_spec, ds_obj, data_context
        )

    ds_obj.datasources = datasources

    return ds_obj


def parse_dataset_spec(name: str, ds_spec: dict, parent: Datasource):
    ds_copy = copy.deepcopy(ds_spec)

    try:
        ds_obj = Dataset(name=name, path=Path(name), **ds_copy, parent=parent)
    except TypeError as e:
        logging.error(
            "Malformed dataset spec: {}\nObject: {}".format(
                e, pprint.pformat(ds_spec, depth=1, indent=4)
            )
        )

        return None

    # bp()
    # ds_copy["org"] = parse_dir_spec(ds_copy["org"])
    ds_obj.org = parse_subset_spec("data", ds_copy["org"], ds_obj)
    # bp()

    return ds_obj


def parse_subset_spec(
    name: str, dir_spec: dict, parent: Union[Dataset, Subset]
) -> Union[None, Subset, RemoteSubset]:
    ds_copy = copy.deepcopy(dir_spec)

    dir_obj: Union[None, Subset, RemoteSubset] = None

    try:
        ds_type = DirectoryType(ds_copy["type"])
        # remove type from the dict; don't need it for unmarshalling past determining dir type
        ds_copy.pop("type", None)
    except ValueError as e:
        logging.error(
            f"Subset type not specified or invalid (type string: '{ds_copy['type']}')"
        )
        return None
    except KeyError as e:
        bp()

    # bp()
    # if set(ds_copy.keys()) == set(Directory.__annotations__):
    if ds_type == DirectoryType.LOCAL:

        try:
            dir_obj = Subset(name=name, path=Path(name), **ds_copy, parent=parent)
        except TypeError as e:
            logging.error(
                "Malformed dir spec: {}\nObject: {}".format(
                    e, pprint.pformat(dir_spec, depth=1, indent=4)
                )
            )

            return None

        subsets = {}
        for subdir_name, subdir in ds_copy["subsets"].items():
            subsets[subdir_name] = parse_subset_spec(subdir_name, subdir, dir_obj)

        dir_obj.subsets = subsets

        resources = {}
        for subfile_name, subfile in ds_copy["resources"].items():
            resources[subfile_name] = parse_resource_spec(
                subfile_name, subfile, dir_obj
            )

        dir_obj.resources = resources

        return dir_obj

    # elif set(ds_copy.keys()) == set(RemoteDirectory.__annotations__):
    if ds_type == DirectoryType.REMOTE:

        try:
            # ds_copy['create_type']= CreateType(ds_copy['create_type'])
            ds_copy["retrieve_type"] = RetrieveType(ds_copy["retrieve_type"])
        except ValueError as e:
            print(e)

        try:
            # bp()
            dir_obj = RemoteSubset(
                name=name,
                path=Path(name),
                # TODO these should be handled later
                subsets=None,
                resources=None,
                **ds_copy,
                parent=parent,
            )
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


def parse_processor_spec(proc_spec: dict) -> Optional[FileFormatProcessor]:
    return None


def parse_resource_spec(
    name: str, file_spec: dict, parent: Subset
) -> Optional[Resource]:
    fs_copy = copy.deepcopy(file_spec)

    # pop out all of the attributes that will need to be separately processed; after this, only POJOs that get passed directly to the resource ctor should remain
    format_name = fs_copy.pop("format_name", None)
    process_spec = fs_copy.pop("process", None)
    file_type = fs_copy.pop("file_type", None)
    retrieve_type = fs_copy.pop("retrieve_type", None)
    create_type = fs_copy.pop("create_type", None)
    template_name = fs_copy.pop("template_name", None)
    name_prefix = fs_copy.pop("name_prefix", None)
    # TODO handle
    retrieve = fs_copy.pop("retrieve", None)
    retain = fs_copy.pop("retain", None)
    template_types = fs_copy.pop("template_types", None)
    unzip = fs_copy.pop("unzip", None)

    processor = parse_processor_spec(process_spec)

    try:
        # fs_copy["file_type"] = FileFormat(fs_copy["file_type"])
        # fs_copy["retrieve_type"] = RetrieveType(fs_copy["retrieve_type"])
        # fs_copy["create_type"] = CreateType(fs_copy["create_type"])
        file_type_ret = FileFormat(file_type)
        retrieve_type_ret = RetrieveType(retrieve_type)
        create_type_ret = CreateType(create_type)

    except ValueError as e:
        print(e)
        bp()
        raise e

    extension_ret = FileFormatToExtensionMap.get(file_type_ret, "UNK")

    file_obj: Optional[Resource] = None

    try:
        if create_type == CreateType.STATIC:
            file_obj = StaticResource(
                name=name,
                path=Path(name),
                **fs_copy,
                parent=parent,
                processor=processor,
                # create_type=create_type_ret,
                file_type=file_type_ret,
                retrieve_type=retrieve_type_ret,
            )
        elif create_type == CreateType.DYNAMIC:
            file_obj = DynamicResource(
                # TODO this abstraction doesn't seem correct
                name=None,
                path=None,
                name_prefix=name_prefix,
                extension=extension_ret,
                **fs_copy,
                parent=parent,
                processor=processor,
                # create_type=create_type_ret,
                file_type=file_type_ret,
                retrieve_type=retrieve_type_ret,
            )
    except TypeError as e:
        logging.error(
            "Malformed file spec: {}\nObject: {}".format(
                e, pprint.pformat(file_spec, depth=1, indent=4)
            )
        )

        raise e
        return None

    if retrieve_type_ret == RetrieveType.GET:
        url_check = urlparse(fs_copy["source"])
        if len(url_check.scheme) == 0 or len(url_check.netloc) == 0:
            logging.error(
                'Malformed file spec (source missing or invalid, in source "{}")\nObject: {}'.format(
                    fs_copy["source"], pprint.pformat(file_spec, depth=1, indent=4)
                )
            )
            return file_obj

    return file_obj


def parse_datasource_file(
    file_path: Path, data_context: DataContext
) -> Optional[Dict[Any, Optional[Datasource]]]:
    datasources = {}

    if exists(file_path) and isfile(file_path):
        with open(file_path) as ds_file:
            # try:
            #     data_json = json.load(ds_file)
            # except json.JSONDecodeError as e:
            #     logging.error(
            #         f"Provided datasource file ({file_path}) is not a JSON file, parse error follows: \n{e}"
            #     )

            #     return None

            try:
                data_yml = yaml.load(ds_file)
            except Exception as e:
                bp()

            for name, ds_json in data_yml.items():
                ds_spec = parse_datasource_spec(name, ds_json, None, data_context)
                datasources[name] = ds_spec

    else:

        logging.error(
            f'Provided datasource file path does not exist or is not a file: "{file_path}"'
        )
        return None

    return datasources


def datasources_from_dir(ds_dir: Path, data_dir: Path) -> Optional[DataCollection]:

    datasources = {}

    def join_ds(fp, ds, ctx):
        parsed_ds = parse_datasource_file(fp, ctx)

        if parsed_ds is not None:
            return {**ds, **parsed_ds}
        else:
            return False

    d_ctx = DataContext(root_path=data_dir)

    if exists(ds_dir) and isdir(ds_dir):
        ds_dir_files = glob(join(ds_dir, "*.yml"))
        logging.info(f"Found files in provided path({ds_dir}): {ds_dir_files}")

        for ds_file_path in ds_dir_files:
            # = parse_datasource_file(ds_path)
            datasources = join_ds(ds_file_path, datasources, d_ctx)

        # print("have datasources")
        # bp()
    else:
        logging.error(
            f'Provided datasource file path does not exist or is not a file: "{ds_file_path}"'
        )
        return False

    return datasources
