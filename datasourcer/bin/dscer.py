import argparse
import logging
from glob import glob
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp

import datasourcer.datasourcer as dscer
import datasourcer.marshalling as dscer_marshall
from datasourcer.datasourcer import DatasetType

# from ... import datasourcer as dscer
# from ... import marshalling as dscer_marshall


# import datasourcer as dscer
# from datasourcer import DatasetType


def process_args(args: argparse.Namespace):

    print(args)

    data_dst_path = Path(args.data_directory)

    datasources = {}

    def join_ds(fp, ds):
        parsed_ds = dscer_marshall.parse_datasource_file(fp)
        if parsed_ds is not None:
            return {**ds, **parsed_ds}
        else:
            return False

    def apply_args(dst: dscer.DatasetType, args: argparse.Namespace):
        if args.validate:
            pass

        if args.download:
            # download(dst, download_types=[])
            dst.download()

    # grab any specified datasources

    # handle datasource_file
    if args.datasource_file is not None:
        ds_file_path = Path(args.datasource_file)
        datasources = join_ds(ds_file_path, datasources)

    # handle datasource dir
    if args.datasource_dir is not None:
        ds_dir_path = Path(args.datasource_dir)

        if exists(ds_dir_path) and isdir(ds_dir_path):
            ds_dir_files = glob(join(ds_dir_path, "*.json"))
            logging.info(f"Found files in provided path: {ds_dir_files}")

            for ds_file_path in ds_dir_files:
                # = parse_datasource_file(ds_path)
                datasources = join_ds(ds_file_path, datasources)

        else:
            logging.error(
                f'Provided datasource file path does not exist or is not a file: "{ds_file_path}"'
            )
            return False

    if args.qualifier:
        retrieved = {}

        for qualifier_arr in args.qualifier:
            have_ret = dscer.retrieve_by_qualifier(qualifier_arr[0])
            if have_ret:
                name, ret = have_ret
                retrieved[name] = ret
            # TODO here

        for name, ret in retrieved.items():
            apply_args(ret, args)

    else:
        for name, datasource in datasources.items():
            apply_args(datasource, args)
            # download_datasource(data_dst_path, datasource)

    # run the download code
    # param_path = "../params/landfire.json"
    # param_path = "../params/nifc.json"
    # param_path = "../params/uscb.json"
    # with open(param_path) as df:
    #     data_json = json.load(df)
    #     # ds_spec = parse_datasource_spec(data_json["LANDFIRE"])
    #     # ds_spec = parse_datasource_spec(data_json["NIFC"])
    #     ds_spec = parse_datasource_spec(data_json["USCB"])
    #     bp()
    #     download_datasource("/home/max/dl_test/", ds_spec)
    # bp()


def run():

    parser = argparse.ArgumentParser(
        description="CLI frontend for describing and downloading datasets."
    )

    parser.add_argument(
        "-dsf",
        "--datasource_file",
        help=" [Optional] Specific datasource file to parse",
        dest="datasource_file",
        type=str,
    )

    parser.add_argument(
        "-dl",
        "--download",
        action="store_true",
        help="Flag; if provided, all specified datasources will be downloaded",
        dest="download",
    )

    parser.add_argument(
        "-v",
        "--validate",
        action="store_true",
        help="Flag; if provided, directories and files will be validated",
        dest="validate",
    )

    parser.add_argument(
        "-p",
        "--print_tree",
        action="store_true",
        help="Flag; if provided, will print the datasource trees after parsing",
        dest="print_tree",
    )

    parser.add_argument(
        "-dsd" "--datasource_dir",
        help="Directory of datasource files to parse (default: current directory). If qualifiers aren't provided, operations will be applied to all datasources in the given directory.",
        type=str,
        default="./",
        dest="datasource_dir",
    )

    parser.add_argument(
        "-ls",
        "--load_static",
        help="Flag; if provided, download static files ",
        action="store_true",
        default=False,
        dest="load_static",
    )

    parser.add_argument(
        "-lsch",
        "--load_scheduled",
        help="Flag; if provided, download scheduled retrieve files",
        action="store_true",
        default=False,
        dest="load_scheduled",
    )

    parser.add_argument(
        "-dd",
        "--data_directory",
        help="Destination directory for downloaded data",
        type=str,
        required=True,
        dest="data_directory",
    )

    parser.add_argument(
        "-q",
        "--qualifier",
        help="Fully qualified identifier for a datasource/dataset/directory/file, case-insensitive and delimited by periods. Multiple qualifiers can be provided, either as a space-delimited list following the argument, or via separate individual arguments. If not provided, all input datasource files will be processed.",
        nargs="*",
        action="append",
        dest="qualifier",
    )

    args = parser.parse_args()
    print(args)
    process_args(args)


if __name__ == "__main__":
    run()
