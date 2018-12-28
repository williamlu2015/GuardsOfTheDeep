import os
from math import inf

import pandas as pd


def main():
    dataset_filename = "../dataset/dataset.csv"
    os.makedirs(os.path.dirname(dataset_filename), exist_ok=True)

    with open(dataset_filename, "w") as dataset_file:
        for i, filename in enumerate(_get_csv_filenames("../data")):
            df = pd.read_csv(filename, sep=",")
            df.to_csv(
                dataset_file, sep=",", header=i == 0, index=False, mode="a"
            )

            print(f"Finished processing {filename}")


def _get_csv_filenames(data_dirname):
    sub_dirnames = _get_immediate_subdirectories(data_dirname)
    sub_dirnames.sort(key=_sub_dirnames_key)

    for sub_dirname in sub_dirnames:
        filenames = _get_immediate_csv_filenames(sub_dirname)
        filenames.sort(key=_filenames_key)

        for filename in filenames:
            yield filename


def _get_immediate_subdirectories(dirname):
    return [
        os.path.join(dirname, subname) for subname in os.listdir(dirname)
        if os.path.isdir(os.path.join(dirname, subname))
    ]


def _get_immediate_csv_filenames(dirname):
    return [
        os.path.join(dirname, subname) for subname in os.listdir(dirname)
        if (os.path.isfile(os.path.join(dirname, subname))
            and subname.endswith(".csv"))
    ]


def _sub_dirnames_key(x):
    basename = os.path.basename(x)
    if basename == "old":
        return -inf

    month, day = map(int, basename.split("-"))
    return 12 * month + day


def _filenames_key(x):
    basename = os.path.basename(x)
    return int(basename[5:-4])


if __name__ == "__main__":
    main()
