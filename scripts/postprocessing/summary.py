import os

import pandas as pd


def generate_summary(result, directory):
    directory = directory or os.getcwd()
    frame = pd.read_json(result)

    def describe(frame, file):
        if len(frame) > 0:
            s = frame.groupby(["function", "cluster"])["time"].describe()
            file.write(f"{s}\n")
            s = frame.groupby(["cluster"])["time"].describe()
            file.write(f"{s}\n")
        else:
            file.write("(empty dataframe)\n")

    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.expand_frame_repr', False):
        with open(os.path.join(directory, "summary.txt"), "w") as f:
            f.write("All results:\n")
            describe(frame, f)
            f.write("Results without first run:\n")
            describe(frame[frame["index"] != 0], f)
