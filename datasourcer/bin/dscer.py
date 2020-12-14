import argparse
import logging
from glob import glob
from os.path import (abspath, basename, dirname, exists, getsize, isdir,
                     isfile, join)
from pathlib import Path
from pdb import set_trace as bp

import datasourcer.datasourcer as dscer
import datasourcer.marshalling as dscer_marshall


def process_args(args: argparse.Namespace):

    print(args)

    data_dst_path = Path(args.data_directory)
    do_download = args.download
    do_download_dynamic = args.download_dynamic
    do_validate = args.validate
    do_process = args.process
    qualifier = args.qualifier
    bpt = args.breakpt
    d_ctx = dscer.DataContext(root_path=data_dst_path)

    datasources: dscer.DataCollection = {}

    def join_ds_file(fp, ds, ctx):
        parsed_ds = dscer_marshall.parse_datasource_file(fp, ctx)

        if parsed_ds is not None:
            # return {**ds, **parsed_ds}
            return join_ds(ds, parsed_ds)

        else:
            return False

    def join_ds(
        d1: dscer.DataCollection, d2: dscer.DataCollection
    ) -> dscer.DataCollection:
        return {**d1, **d2}

    def apply_args(
        dst: dscer.DatasetType, validate: bool, download: bool, process: bool
    ):
        if validate:
            pass

        if download:
            # download(dst, download_types=[])
            dst.download(validate_existing=True, reload_unconfirmable=False)

        if process:
            dst.process()

    def wrap_apply(
        validate: bool, download: bool, process: bool, download_dynamic: bool
    ):
        def fn(tv: dscer.Traversable, depth=0):
            if download_dynamic and isinstance(tv, dscer.DynamicResource):
                if tv.can_download():
                    tv.retrieve_snapshot()

            if (
                download
                and (
                    isinstance(tv, dscer.Downloadable)
                    or isinstance(tv, dscer.RemoteSubset)
                )
                and not isinstance(tv, dscer.DynamicResource)
            ):
                if tv.can_download():
                    tv.download(level=depth)

            if process and isinstance(tv, dscer.Processable):
                if tv.can_process():
                    tv.process()

        return fn

    apply_fn = wrap_apply(do_validate, do_download, do_process, do_download_dynamic)

    # grab any specified datasources

    # handle datasource_file
    if args.datasource_file is not None:
        ds_file_path = Path(args.datasource_file)
        datasources = join_ds_file(ds_file_path, datasources, d_ctx)

    # handle datasource dir
    if args.datasource_dir is not None:
        dir_dsources = dscer_marshall.datasources_from_dir(
            args.datasource_dir, data_dst_path
        )

        if dir_dsources:
            datasources = join_ds(dir_dsources, datasources)

    # if we have a qualifier, traverse & apply by it
    if args.qualifier:
        retrieved = {}

        for qualifier_arr in qualifier:
            have_ret = dscer.retrieve_by_qualifier(datasources, qualifier_arr[0])
            if have_ret:
                retrieved[have_ret.name] = have_ret

        for name, ret in retrieved.items():
            ret.apply(apply_fn)

    else:
        for name, datasource in datasources.items():
            datasource.apply(apply_fn)

    if bpt:
        bp()

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
        "-dld",
        "--download_dynamic",
        action="store_true",
        help="Flag; if provided, all specified dynamic resources will be downloaded ('snapshots' of live resources)",
        dest="download_dynamic",
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
        "--process",
        action="store_true",
        help="Flag; if provided, directories and files will be processed",
        dest="process",
    )

    parser.add_argument(
        "-P",
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

    parser.add_argument(
        "-bp",
        "--breakpoint",
        help="Trigger a PDB breakpoint after parsing is complete.",
        action="store_true",
        dest="breakpt",
    )

    args = parser.parse_args()
    print(args)
    process_args(args)


if __name__ == "__main__":
    run()
