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
import zipfile
from collections import deque, namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from glob import glob
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import requests

logging.basicConfig(level=logging.INFO)

# not a useful def here
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


class Downloadable:
    def download(
        self,
        parent_dir: Path,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
    ) -> bool:
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


# these dataclass definitions also define the layout of the input datasource JSON files
@dataclass
class File(Traversable, Downloadable):
    name: str
    file_type: FileFormat
    retrieve_type: RetrieveType
    source: Optional[str]
    path_name: Path
    description: Optional[str]
    unzip: Optional[Path]
    create_type: CreateType
    retrieve: bool
    retain: bool
    template_types: Optional[List[TemplateType]]

    parent: "Directory"

    # No traversal precedence (no nested structures here)
    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:
        return self

    def download(
        self,
        parent_dir: Path,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
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

        level_info('Downloading file "{}"'.format(self.path_name), level)

        if self.retrieve_type == RetrieveType.GET:
            level_info("GET {}".format(self.source), level)

            if self.source is None or len(self.source) == 0:
                level_info("No source specified; skipping file spec", level)
                return False

            # make the directories leading up to this file, if they don't already exist
            os.makedirs(parent_dir, exist_ok=True)

            file_path = Path(join(parent_dir, self.path_name))

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
class Directory(Traversable):
    name: str
    path_name: Path
    create_type: CreateType
    dirs: Dict[str, "Directory"]
    files: Dict[str, File]
    type: DirectoryType

    parent: Union["Dataset", "Directory"]

    # Files first
    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # base case; nothing left in the traversal stack, so this is (hopefully) what we were looking for
        if len(traverse_stack) == 0:
            return self

        target = traverse_stack.popleft()

        have_file = check_dict(target, self.files, match_case=match_case)

        # files can't traverse, so don't try (and if there's still something left in the traverse stack, skip it)
        if have_file and len(traverse_stack) == 0:
            return have_file
        elif len(traverse_stack) > 0:
            # don't try to traverse a file; fail safely and make the user specify
            return None

        have_dir = check_dict(target, self.dirs, match_case=match_case)

        if have_dir:
            return have_dir.traverse(traverse_stack, match_case=match_case)

        return None

    def download(
        self,
        parent_dir: Path,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
    ) -> None:
        dir_path = Path(join(parent_dir, self.path_name))

        for subdir_name, subdir in self.dirs.items():
            # download_directory(dir_path, subdir, level=level + 1)
            subdir.download(dir_path, level=level + 1)

        for subfile_name, subfile in self.files.items():
            # download_file(dir_path, subfile, level=level + 1)
            subfile.download(dir_path, level=level + 1)


@dataclass
class RemoteDirectory(Traversable):
    name: str
    path_name: Path
    retrieve_type: RetrieveType
    source: str

    parent: Union["Dataset", Directory]

    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # base case; nothing left in the traversal stack, so this is (hopefully) what we were looking for
        if len(traverse_stack) == 0:
            return self
        else:
            # can't (yet) request a subfile for a remote directory
            return None

    def download(
        self,
        parent_dir: Path,
        level: int = 0,
        validate_existing: bool = True,
        reload_unconfirmable: bool = True,
    ) -> None:
        # download_remote_directory(dir_path, directory, level=level)
        # def download_remote_directory(
        #     parent_dir: Path,
        #     dir_spec: RemoteDirectory,
        #     level: int = 0,
        #     validate_existing: bool = True,
        #     reload_unconfirmable: bool = True,
        # ) -> None:
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
class Dataset(Traversable):
    name: str
    description: Optional[str]
    org: Directory

    parent: "Datasource"

    def traverse(
        self, traverse_stack: deque, match_case: bool = False
    ) -> Optional["DatasetType"]:

        # forward the traversal to the (org)anization of the dataset (i.e. the dir)
        return self.org.traverse(traverse_stack, match_case=match_case)

    def download(self, parent_dir: str, level: int = 0):
        # def download_dataset(parent_dir: Path, dataset: Dataset, level: int = 1):

        logging.info('{}Downloading dataset "{}"'.format("\t" * level, self.name))

        # used to use the dataset name as a directory name directly, but instead this is specified in the org dir
        # dir_path = join(parent_dir, dataset.name)
        dir_path = parent_dir

        # download_directory(dir_path, dataset.org, level=level + 1)
        self.org.download(dir_path, level=level + 1)


@dataclass
class Datasource(Traversable):
    name: str
    description: Optional[str]
    datasets: Dict[str, Dataset]
    datasources: Dict[str, "Datasource"]

    parent: Optional["Datasource"]

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

    def download(self, parent_dir: Path, level: int = 0):
        ds_path = Path(join(parent_dir, self.name))

        logging.info('{}Downloading datasource "{}"'.format("\t" * level, self.name))

        for subsource_name, subsource in self.datasources.items():
            subsource.download(ds_path, subsource, level=level + 1)

        for dataset_name, dataset in self.datasets.items():
            dataset.download(ds_path, dataset, level=level + 1)


DatasetType = Union[Datasource, Dataset, Directory, RemoteDirectory, File]
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

    if exists and not is_valid:
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


def retrieve_by_qualifier(
    spec: DataCollection, qualifier: str, delimiter: str = ".", match_case: bool = False
) -> Tuple[str, Optional[DatasetType]]:

    qual_parts = qualifier.split(delimiter)
    qual_stack = deque(qual_parts)

    print(qual_parts, qual_stack)

    first = qual_stack.popleft()

    have_first = check_dict(first, spec, match_case=match_case)

    name, retrieved = spec.traverse(qual_stack, match_case=match_case)

    print(name, retrieved)

    return (name, retrieved)
