#! /usr/local/bin/python
# Requires: rechunker==0.5.2

import shutil
from pathlib import Path

# import dask.array as dsa
from dask.diagnostics import ProgressBar

from rechunker import rechunk
from pytools import HedwigZarrImages
import zarr

src = "/gs1/Scratch/Anish_czi_examples/"
RECHUNK_SIZE = 256


def rechunk_zarr_rechunker(dirname, fname, exc="python") -> None:
    source_store = Path(dirname) / fname

    images = HedwigZarrImages(source_store, read_only=False)
    # assumption is that, zarr arrays are going to have dims: TCZYX
    # FIXME no memory limit , limiting memory: big files cannot be rechunked
    max_mem = "100000000TB"
    for _, image in images.series():
        print(f"Rechunking {image.zarr_group.path}...")
        # each image is a zarr group
        target_chunks = dict()
        target_options = dict()
        # to rechunk a group, we need array names for each array
        ome_img = image._ome_ngff_multiscales()
        for dataset in ome_img["datasets"]:
            arr = image.zarr_group[dataset["path"]]
            array_dims = len(arr.shape)
            possible_rechunk = [RECHUNK_SIZE] * array_dims
            target_chunks[arr.name] = [
                min(p, a) for p, a in zip(possible_rechunk, arr.shape)
            ]
            if arr.chunks == tuple(target_chunks[arr.name]):
                target_chunks[arr.name] = None
            target_options[arr.name] = {"compressor": arr.compressor}

        if not any(target_chunks.values()):
            continue

        target_store = Path(dirname) / f"target_{fname}_{image.zarr_group.path}.zarr"
        temp_store = Path(dirname) / f"tmp_{fname}_{image.zarr_group.path}.zarr"

        if target_store.exists():
            # remove zarr stores or it won't work
            shutil.rmtree(target_store)
        if temp_store.exists():
            shutil.rmtree(temp_store)

        source_group = zarr.open(source_store.as_posix())
        rechunk_plan = rechunk(
            source_group,
            target_chunks,
            max_mem,
            target_store,
            target_options=target_options,
            temp_store=temp_store,
            executor=exc,
        )
        with ProgressBar():
            rechunk_plan.execute()

        # shutil.copytree(
        #     target_store / f"{image.zarr_group.path}", source_store / f"{image.zarr_group.path}", dirs_exist_ok=True
        # )


def rechunk_zarr_rechunker_dsa(dirname, fname):
    rechunk_zarr_rechunker(dirname, fname, "dask")


def rechunk_zarr_pytools(dirname, fname) -> None:
    zarr_fp = Path(dirname) / fname
    images = HedwigZarrImages(zarr_fp, read_only=False)
    for _, image in images.series():
        print(f"Rechunking {image.zarr_group.path}...")
        image.rechunk(RECHUNK_SIZE, in_memory=True)


def get_chunk_size(dirname, fname):
    group = zarr.open((Path(dirname) / fname).as_posix())
    print(group.info)
    print(zarr.tree(group))
    print(group["0"].info)
    print(group["0"]["0"].info)


def call():
    import sys

    # fname = "KC_M3_S2_ReducedImageSubset_pytools.zarr"
    # fname = "KC_M3_S2_ReducedImageSubset_rechunker_dsa.zarr"
    # fname = "KC_M3_S2_ReducedImageSubset_rechunker.zarr"

    # fname = "OM_P1_S2_orig.zarr"
    fname = "OM_P1_S2_pytools.zarr"
    fname = "OM_P1_S2_rechunker.zarr"
    fname = "OM_P1_S2_rechunker_dsa.zarr"
    # fname = "target_3.zarr"
    # fname = f"target_{fname}_0.zarr"

    print("Rechunking", fname)

    if sys.argv[1:]:
        get_chunk_size(src, fname)
    else:
        rechunk_zarr_rechunker_dsa(src, fname)


if __name__ == "__main__":
    import timeit

    print(
        "It took",
        timeit.timeit("call()", setup="from __main__ import call", number=1),
        "seconds",
    )
