import argparse
import copy
import ftplib
import json
import logging
import math
import os
import pprint
import sys
import time
from collections import deque, namedtuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from glob import glob
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse
from zipfile import ZipFile

import requests

logging.basicConfig(level=logging.INFO)

# not a useful def here (though does carry useful semantics
# class DatasetType(str, Enum):
#     ARCHIVE="ARCHIVE",
#     LIVE="LIVE",
#     ONGOING="ONGOING"


# self-explanatory; various types of file formats
class FileFormat(str, Enum):

    # zipfiles will have a corresponding 'unzip' attribute, specifying the decompressed directory name
    ZIP = ("ZIP",)
    CSV = ("CSV",)
    GEOJSON = ("GEOJSON",)
    JSON = ("JSON",)
    ARCGRID = ("ARCGRID",)
    SHAPEFILE = ("SHAPEFILE",)
    KML = ("KML",)


class FileFormatProcessor:
    def process(self, resource: "StaticResource") -> bool:
        return False


@dataclass
class ZipfileProcessor(FileFormatProcessor):
    unzip_dir: Path

    def process(self, resource: "StaticResource") -> bool:
        success = True

        zf_path = resource.path
        out_path = resource.build_parent_path() / self.unzip_dir

        try:
            with open(zf_path, "rb") as zf_bin:
                zf = ZipFile(zf_bin)
                zf.extractall(path=out_path)

        except Exception as e:
            print(f"Failed to unzip {resource} using {self}")
            raise e

        return success


FileFormatToExtensionMap: Dict[FileFormat, str] = {
    FileFormat.ZIP: "zip",
    FileFormat.CSV: "csv",
    FileFormat.GEOJSON: "geo.json",
    FileFormat.JSON: "json",
    FileFormat.ARCGRID: "grid",
    FileFormat.SHAPEFILE: "shp",
    FileFormat.KML: "kml",
}


# map of file formats to available processors
FileFormatToProcessorMap: Dict[FileFormat, Optional[FileFormatProcessor]] = {
    FileFormat.ZIP: ZipfileProcessor,
    FileFormat.CSV: None,
    FileFormat.GEOJSON: None,
    FileFormat.JSON: None,
    FileFormat.ARCGRID: None,
    FileFormat.SHAPEFILE: None,
    FileFormat.KML: None,
}


# file retrieval method
class RetrieveType(str, Enum):

    # standard GET request; source is a url (files only)
    GET = ("GET",)

    # FTP server request; source is an ftp url (directories or files)
    FTP = ("FTP",)

    # manually-populated (i.e. "by hand") data or directory (e.g. for an unscriptable archive source)
    MANUAL = ("MANUAL",)

    # not retrieved; instead, created as part of the defined hierarchy (for directories; files will always be retrieved)
    NONE = ("NONE",)


# object lifecycle or creation scope; different creation scopes will be retrieved in different contexts
class CreateType(str, Enum):

    # statically-created, should always exist, & will be retrieved in a static data pull
    STATIC = ("STATIC",)

    # generated in some way, typically for live data sources (& typically is templated, e.g. via datetime); will not be retrieved in a static data pull, should be handled in a cron-style ETL stage instead
    DYNAMIC = ("DYNAMIC",)


# for figuring out directory types when unmarshalling JSON
class DirectoryType(str, Enum):

    # locally-created directory
    LOCAL = ("LOCAL",)

    # directory retrieved from elsewhere (e.g. via FTP)
    REMOTE = ("REMOTE",)


class ResourceType(str, Enum):

    # static resources, i.e. those that will not change after definition and retrieval; access time does not matter and will not influence the content
    STATIC = "STATIC"

    # dynamic resources, i.e. those that may change after retrieval; access time matters, and will dictate the content
    DYNAMIC = "DYNAMIC"


# necessary?
# class DataRetrieveSpecType(str, Enum):
#     DATASOURCE,
#     DATASET,
#     DIRECTORY,
#     FTP_DIRECTORY,
#     FILE,


class Traversable:
    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:
        pass

    def get_traversable_children(self):
        pass

    def apply(self, fn: Callable, depth=0):
        fn(self, depth=depth)

        for child in self.get_traversable_children():
            child.apply(fn, depth=depth + 1)

    def build_parent_path(self) -> Path:
        pass

    def build_path(self) -> Path:
        pass


class Downloadable:
    def can_download(self) -> bool:
        pass

    def download(
        self,
        parent_dir: Optional[Path] = None,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
        name: Optional[Path] = None,
    ) -> bool:
        pass


class Processable:
    def can_process(self) -> bool:
        pass

    def process(self) -> bool:
        pass


class TemplateType(str, Enum):
    DATETIME = "DATETIME"


def datasource_repr(self):
    return pprint.pformat(self._asdict(), indent=1)


def check_dict(key: str, d: Dict[str, Any], match_case: bool = False) -> Optional[Any]:
    dict_set = {k.lower() if not match_case else k: k for k in d}

    check = dict_set.get(key)

    if check is not None:
        return d.get(check)

    return None


@dataclass
class DataContext:
    root_path: Path


# these dataclass definitions also define the layout of the input datasource JSON files
@dataclass
class Resource(Traversable, Downloadable, Processable):
    name: str
    file_type: FileFormat
    retrieve_type: RetrieveType
    source: Optional[str]
    path: Path
    description: Optional[str]
    processor: FileFormatProcessor
    # this should be part of a middleware or something
    # unzip: Optional[Path]

    # dynamic vs. static
    # create_type: CreateType
    # retrieve: bool
    # retain: bool
    # template_types: Optional[List[TemplateType]]
    # parent: Union["Directory", "Subset"]
    parent: "Subset"

    def __repr__(self):
        return f'StaticResource("{self.name}", source: {self.source})'

    # No traversal precedence (no nested structures here)
    # TODO shouldn't be traversing past a file; if traverse_stack stil has elements, return error
    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:
        return self

    def build_parent_path(self) -> Path:
        return self.parent.build_path()

    def build_path(self) -> Path:
        return self.build_parent_path() / self.path

    def get_traversable_children(self):
        # resource has no traversable children (should it even be considered traversable?)
        return []

    def can_download(self) -> bool:
        # a place to download from, a download method, and a place to put the download
        return (
            self.source is not None
            and self.retrieve_type is not None
            and self.path is not None
        )

    def download(
        self,
        parent_dir: Optional[Path] = None,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = False,
        name: Optional[Path] = None,
    ) -> bool:
        # downloads a file from the file spec into the file dictated by parent_dir + file_spec.path_name
        # returns True if a valid file is believed to exist, False otherwise
        # def download_file(
        #     parent_dir: Path,
        #     file_spec: File,
        #     level: int = 0,
        #     check_exist: bool = True,
        #     validate_existing_file: bool = True,
        #     reload_unconfirmable: bool = False,
        # ) -> bool:
        # logging.info("Downloading file \"{}\"".format('\t' * level, file_spec.path_name))

        # handle default case for no-override filename
        if name is None:
            name = self.path

        # handle default case for no-override parent path
        if parent_dir is None:
            parent_dir = self.build_parent_path()

        level_info('Downloading file "{}"'.format(name), level)

        if self.retrieve_type == RetrieveType.GET:
            level_info("GET {}".format(self.source), level)

            if self.source is None or len(self.source) == 0:
                level_info("No source specified; skipping file spec", level)
                return False

            # make the directories leading up to this file, if they don't already exist
            os.makedirs(parent_dir, exist_ok=True)

            file_path = Path(join(parent_dir, name))

            # request the headers here, with the accepted compression to be 'none' (so that chunking size lines up with content-length here)
            head = requests.head(
                self.source, stream=True, headers={"Accept-Encoding": "identity"}
            )

            # check if the file already exists
            if validate_existing:

                # TODO
                filesize_remote = None

                (exists, is_valid) = validate_file(
                    file_path, remote_filesize=filesize_remote, level=level
                )

                do_download = check_validation_policy(
                    exists,
                    is_valid,
                    level=level,
                    reload_unconfirmable=reload_unconfirmable,
                )

                if not do_download:
                    if exists:
                        return True
                    else:
                        return False

            level_info("Into {}".format(file_path), level)

            # request the file here; we're going to look at the Content-Length header size and want some fine-grained control over how we download this (e.g. for logging purposes), so enable streaming
            get_chunked(self.source, Path(file_path), level=level)

            return True

        elif self.retrieve_type == RetrieveType.FTP:
            level_info("FTP {}".format(self.source), level)
        if self.retrieve_type == RetrieveType.MANUAL:
            level_info("Manual (no-op)", level)

        # catch-all false, retrieve_type not handled
        level_info('Retrieve type not handled ("{}")'.format(self.retrieve_type), level)
        return False


@dataclass
class StaticResource(Resource):
    name: str
    path: Path


@dataclass
class DynamicResource(Resource):
    name_prefix: str
    extension: str

    def can_download(self) -> bool:
        # a place to download from, and a download method; path is determined by timestamp / fmt
        return self.source is not None and self.retrieve_type is not None

    def format_name(self, timestamp: datetime):
        return (
            f"{self.name_prefix}.{timestamp.strftime('%Y_%m_%d_%H%M')}.{self.extension}"
        )

    def retrieve_snapshot(self, parent_dir: Optional[Path] = None):
        timestamp = datetime.now()

        file_name = self.format_name(timestamp)

        self.download(parent_dir=parent_dir, name=file_name)


@dataclass
class Subset(Traversable, Processable):
    name: str
    path: Path
    create_type: CreateType
    subsets: Dict[str, "Subset"]
    resources: Dict[str, StaticResource]
    parent: Union["Dataset", "Subset"]

    def __repr__(self):
        return f'Subset("{self.name}", Subsets: {list(self.subsets.keys())}, Resources: {list(self.resources.keys())})'

    def build_parent_path(self) -> Path:
        return self.parent.build_path()

    def build_path(self) -> Path:
        return self.build_parent_path() / self.path

    # Files first
    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # base case; nothing left in the traversal stack, so this is (hopefully) what we were looking for
        if len(traverse_stack) == 0:
            return self

        target = traverse_stack.popleft()

        have_file = check_dict(target, self.resources, match_case=match_case)

        # files can't traverse, so don't try (and if there's still something left in the traverse stack, skip it)
        if have_file and len(traverse_stack) == 0:
            return have_file
        elif len(traverse_stack) > 0:
            # don't try to traverse a file; fail safely and make the user specify
            return None

        have_dir = check_dict(target, self.subsets, match_case=match_case)

        if have_dir:
            return have_dir.traverse(traverse_stack, match_case=match_case)

        return None

    def get_traversable_children(self) -> List[Traversable]:
        children: List[Traversable] = []

        for subset_name, subset in self.subsets.items():
            children.append(subset)

        for subresource_name, subresource in self.resources.items():
            children.append(subresource)

        return children

    # def download(
    #     self,
    #     parent_dir: Optional[Path] = None,
    #     level: int = 0,
    #     validate_existing: bool = True,
    #     reload_unconfirmable: bool = True,
    #     name: Optional[str] = None,
    # ) -> None:
    #     if parent_dir is None:
    #         parent_dir = self.build_parent_path()

    #     dir_path = Path(join(parent_dir, self.path))

    #     for subdir_name, subdir in self.subsets.items():
    #         # download_directory(dir_path, subdir, level=level + 1)
    #         try:
    #             subdir.download(
    #                 level=level + 1,
    #                 validate_existing=validate_existing,
    #                 reload_unconfirmable=reload_unconfirmable,
    #             )
    #         except TypeError as e:
    #             print(e)
    #             bp()

    #     for subfile_name, subfile in self.resources.items():
    #         # download_file(dir_path, subfile, level=level + 1)
    #         subfile.download(
    #             dir_path,
    #             level=level + 1,
    #             validate_existing=validate_existing,
    #             reload_unconfirmable=reload_unconfirmable,
    #         )

    def get_file_by_index(self, index):
        filenames = list(self.resources.keys())

        try:
            key = filenames[index]
        except IndexError as e:
            return None

        obj = self.resources.get(key)

        return obj


@dataclass
class RemoteSubset(Subset):
    name: str
    path: Path
    retrieve_type: RetrieveType
    source: str

    def __repr__(self):
        return f'RemoteSubset("{self.name}", source: self.source)'

    def build_parent_path(self) -> Path:
        return self.parent.build_path()

    def build_path(self) -> Path:
        return self.build_parent_path() / self.path

    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # base case; nothing left in the traversal stack, so this is (hopefully) what we were looking for
        if len(traverse_stack) == 0:
            return self
        else:
            # can't (yet) request a subfile for a remote directory
            return None

    def get_traversable_children(self) -> List[Traversable]:
        # remote subset has no children, as it's self-contained
        return []

    def download(
        self,
        parent_dir: Optional[Path] = None,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
        name: Optional[Path] = None,
    ) -> None:
        # download_remote_directory(dir_path, directory, level=level)
        # def download_remote_directory(
        #     parent_dir: Path,
        #     dir_spec: RemoteDirectory,
        #     level: int = 0,
        #     validate_existing: bool = True,
        #     reload_unconfirmable: bool = True,
        # ) -> None:

        if parent_dir is None:
            parent_dir = self.build_parent_path()

        if self.retrieve_type == RetrieveType.GET:
            level_error("Retrieve type GET not impl.; object: {}".format(self), level)

        elif self.retrieve_type == RetrieveType.FTP:
            url = urlparse(self.source)
            level_info(
                "Retrieving directory at {} from path {} into {}".format(
                    url.netloc, url.path, parent_dir
                ),
                level,
            )

            os.makedirs(parent_dir, exist_ok=True)

            with ftplib.FTP(host=url.netloc) as ftp:
                ftp.login()
                level_info("FTP connection successful", level)
                remote_files = ftp.nlst(url.path)

                for remote_file in remote_files:
                    remote_path = Path(remote_file)
                    file_path = Path(join(parent_dir, remote_path.name))

                    level_info("Downloading {}...".format(remote_path.name), level)

                    if validate_existing:
                        remote_size = ftp.size(remote_file)

                        (exists, is_valid) = validate_file(
                            file_path, remote_filesize=remote_size, level=level
                        )

                        do_download = check_validation_policy(
                            exists,
                            is_valid,
                            level=level,
                            reload_unconfirmable=reload_unconfirmable,
                        )

                        if not do_download:
                            continue

                    with open(file_path, "wb") as ftp_file:
                        ftp.retrbinary("RETR {}".format(remote_file), ftp_file.write)

                    level_info(
                        "Finished downloading {} (size {})".format(
                            remote_path.name, convert_size(getsize(file_path))
                        ),
                        level,
                    )

        elif self.retrieve_type == RetrieveType.MANUAL:
            level_error("Retrieve type MANUAL no-op; object: {}".format(self), level)
        elif self.retrieve_type == RetrieveType.NONE:
            level_error("Retrieve type NONE no-op.; object: {}".format(self), level)
        else:
            level_error("Retrieve type unmatched; object: {}".format(self), level)


@dataclass
class Dataset(Traversable, Downloadable, Processable):
    name: str
    path: Path
    description: Optional[str]
    org: Subset

    parent: "Datasource"

    def __repr__(self):
        return f'Dataset("{self.name}", Org: {self.org})'

    def build_parent_path(self) -> Path:
        return self.parent.build_path()

    def build_path(self) -> Path:
        return self.build_parent_path() / self.path

    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # forward the traversal to the (org)anization of the dataset (i.e. the dir)
        return self.org.traverse(traverse_stack, match_case=match_case)

    def get_traversable_children(self) -> List[Traversable]:
        # only 1 traversable child; the org subset
        return [self.org]

    def download(
        self,
        parent_dir: Optional[Path] = None,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
        name: Optional[Path] = None,
    ):
        # def download_dataset(parent_dir: Path, dataset: Dataset, level: int = 1):

        logging.info('{}Downloading dataset "{}"'.format("\t" * level, self.name))

        # used to use the dataset name as a directory name directly, but instead this is specified in the org dir
        # dir_path = join(parent_dir, dataset.name)
        if parent_dir is None:
            parent_dir = self.build_parent_path()

        dir_path = parent_dir

        # download_directory(dir_path, dataset.org, level=level + 1)
        self.org.download(
            dir_path,
            level=level + 1,
            validate_existing=validate_existing,
            reload_unconfirmable=reload_unconfirmable,
        )


@dataclass
class Datasource(Traversable, Downloadable, Processable):
    name: str
    path: Path
    description: Optional[str]
    datasets: Dict[str, Dataset]
    datasources: Dict[str, "Datasource"]

    data_context: DataContext

    parent: Optional["Datasource"]

    def __repr__(self):
        return f'Datasource("{self.name}", Datasets: {list(self.datasets.keys())}, Subsources: {list(self.datasources.keys())})'

    def build_parent_path(self) -> Path:

        if self.parent is None:
            return self.data_context.root_path

        elif self.parent is not None:
            # return self.build_parent_path()
            return self.parent.build_path()

    def build_path(self):
        return self.build_parent_path() / self.path

    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # base case; nothing left in the traversal stack, so this is (hopefully) what we were looking for
        if len(traverse_stack) == 0:
            return self

        target = traverse_stack.popleft()

        # try datasources first
        have_dsource = check_dict(target, self.datasources, match_case=match_case)

        if have_dsource:
            return have_dsource.traverse(traverse_stack)

        # next, datasets

        have_dset = check_dict(target, self.datasets, match_case=match_case)

        if have_dset:
            return have_dset.traverse(traverse_stack, match_case=match_case)

        return None

    def get_traversable_children(self) -> List[Traversable]:
        children = []

        for subsource_name, subsource in self.datasources.items():
            children.append(subsource)

        for dataset_name, dataset in self.datasets.items():
            children.append(dataset)

        return children

    def download(
        self,
        parent_dir: Optional[Path] = None,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
        name: Optional[Path] = None,
    ):
        if parent_dir is None:
            parent_dir = self.build_parent_path()

        ds_path = Path(join(parent_dir, self.name))

        logging.info('{}Downloading datasource "{}"'.format("\t" * level, self.name))

        for subsource_name, subsource in self.datasources.items():
            subsource.download(
                ds_path,
                subsource,
                level=level + 1,
                validate_existing=validate_existing,
                reload_unconfirmable=reload_unconfirmable,
            )

        for dataset_name, dataset in self.datasets.items():
            dataset.download(
                ds_path,
                dataset,
                level=level + 1,
                validate_existing=validate_existing,
                reload_unconfirmable=reload_unconfirmable,
            )


DatasetType = Union[Datasource, Dataset, Subset, RemoteSubset, StaticResource]
DataCollection = Dict[str, DatasetType]


# from https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
def convert_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def check_validation_policy(
    exists: bool,
    is_valid: Optional[bool],
    level: int = 0,
    reload_unconfirmable: bool = True,
) -> bool:
    if not exists:
        level_info("File doesn't exist; downloading", level)
        return True

    if exists and is_valid:
        level_info("Skipping download", level)
        return False

    if exists and is_valid is False:
        level_info("File exists, but isn't valid", level)
        return True

    if exists and is_valid is None:
        if is_valid is None and reload_unconfirmable is True:
            level_info("Unconfirmable file; downloading", level)
            return True
        elif is_valid is None and reload_unconfirmable is False:
            level_info("Unconfirmable file; skipping (assumed good)", level)
            return False

    level_info(
        "Validation policy not handled (exists? {}; is_valid? {})".format(
            exists, is_valid
        ),
        level,
    )
    return False


def get_filesize_from_headers(
    headers: requests.structures.CaseInsensitiveDict,
    log: bool = True,
    level: int = 0,
) -> Optional[int]:
    def log_level_if(string, level):
        if log:
            level_info(string, level)

    # try to figure out if we can get the uncompressed filesize from the source link; with gzip-encoding, this gives compressed transfer size
    # there's at least one more path, requesting with Accept-Encoding: "" and seeing if the transfer-encoding isn't chunked
    if headers.get("Content-Encoding") in ["gzip", "compress", "deflate", "br"]:
        log_level_if(
            "Content-Encoding (\"{}\") is compressed; can't determine an uncompressed content length & can't validate file".format(
                headers["Content-Encoding"]
            ),
            level,
        )
        return None

    if headers.get("Content-Length") is None:
        log_level_if(
            "Content-Length is unknown; can't determine a content length & can't validate file",
            level,
        )
        return None

    # TODO this probably warrants a bit more careful treatment
    filesize_src = int(headers["Content-Length"])

    return filesize_src


# returns (exists, valid) tuple where exists is a bool and valid is a bool or None for indeterminate/unknown
# def validate_file(headers, file_path, log=True, level=0, validate_size=True):
def validate_file(
    file_path: Path,
    remote_filesize: Optional[int] = None,
    file_hash: Optional[str] = None,
    log: bool = True,
    level: int = 0,
    validate_size: bool = True,
) -> Tuple[bool, Optional[bool]]:
    def log_level_if(string, level):
        if log:
            level_info(string, level)

    if not exists(file_path):
        return (False, False)

    filesize = getsize(file_path)

    if remote_filesize is not None:
        # file size validation logic block; various checks and logs based on function params and local vs. source file sizes
        if filesize == remote_filesize:
            log_level_if(
                "File already exists: {} vs. expected {} (size match; skip retrieve)".format(
                    convert_size(filesize), convert_size(remote_filesize)
                ),
                level,
            )
            return (True, True)
        else:
            log_level_if(
                "File already exists: {} vs. expected {} (mismatch; retrieve from source)".format(
                    convert_size(filesize), convert_size(remote_filesize)
                ),
                level,
            )
            return (True, False)

    else:
        return (True, None)


def get_chunked(
    source_url: str,
    dst_path: Path,
    log: bool = True,
    level: int = 0,
    cp_percent: float = 0.1,
) -> None:
    resp = requests.get(source_url, stream=True)

    # grabbing file size and generating the "size string" (to reuse later, esp. since the no-size logic would clutter later code)
    filesize_src = None
    size_str = "??"
    if resp.headers.get("Content-Length") is not None and resp.headers.get(
        "Content-Encoding"
    ) not in ["gzip", "compress", "deflate", "br"]:
        filesize_src = int(resp.headers.get("Content-Length"))
        size_str = convert_size(filesize_src)
        level_info("File size is {}".format(size_str), level)
    else:
        level_info("File size is unknown", level)

    # 'checkpoint' chunk sizing, for logging purposes (every 10% by default)
    cp_chunk_size = (
        int(cp_percent * filesize_src) if filesize_src is not None else 1024 ** 2 * 100
    )
    cp_chunk_idx = 0

    with open(dst_path, "wb") as out_file:
        chunk_accum = 0
        for chunk in resp.iter_content(chunk_size=1024 ** 2):
            out_file.write(chunk)

            chunk_accum = chunk_accum + len(chunk)

            if cp_chunk_idx * cp_chunk_size < chunk_accum:
                cp_chunk_idx = cp_chunk_idx + 1

                if log:
                    level_info(
                        "{} / {} ({}%)".format(
                            convert_size(chunk_accum),
                            size_str,
                            int(chunk_accum / filesize_src * 100)
                            if filesize_src is not None
                            else "??",
                        ),
                        level,
                    )


def level_info(msg: Any, level: int) -> None:
    level_log(logging.info, msg, level)


def level_error(msg: Any, level: int) -> None:
    level_log(logging.error, msg, level)


def level_log(log_fn: Callable[..., None], msg: Any, level: int = 0) -> None:
    log_fn("{}{}".format("\t" * level, msg))


def parse_datasource_directory(dir: Path) -> dict:
    for root, dirs, files in os.walk(dir):
        pass

    return {}


# TODO support a more flexible qualifier system, e.g. with quotes? e.g., a.b."longer c.with.delimiters.as.part.of.identifier.kml".d
def retrieve_by_qualifier(
    spec: DataCollection, qualifier: str, delimiter: str = ".", match_case: bool = False
) -> Tuple[str, Optional[DatasetType]]:

    qual_parts = qualifier.split(delimiter)
    qual_stack = deque(qual_parts)

    print(qual_parts, qual_stack)

    first = qual_stack.popleft()

    have_first = check_dict(first, spec, match_case=match_case)

    if not have_first:
        return None

    if len(qual_stack) == 0:
        return have_first

    # name, retrieved = have_first.traverse(qual_stack, match_case=match_case)
    retrieved = have_first.traverse(qual_stack, match_case=match_case)

    # print("retrieved:", retrieved)
    # bp()

    return retrieved
