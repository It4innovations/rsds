import os

import pandas as pd
import seaborn as sns


def create_plot(frame, plot_fn):
    def extract(fn):
        items = list(fn.split("-"))
        for i in range(len(items)):
            try:
                num = float(items[i])
                items[i] = num
            except:
                pass
        return tuple(items)

    clusters = sorted(set(frame["cluster"]))
    functions = sorted(set(frame["function"]), key=extract)

    def plot(data, **kwargs):
        plot_fn(data, clusters, **kwargs)

    g = sns.FacetGrid(frame, col="function", col_wrap=4, col_order=functions, sharey=False)
    g = g.map_dataframe(plot)
    g = g.add_legend()
    g.set_ylabels("Time [ms]")
    g.set(ylim=(0, None))
    g.set_xticklabels(rotation=90)
    return g


def generate_charts(result, directory):
    directory = directory or os.getcwd()
    frame = pd.read_json(result)

    def plot_box(data, clusters, **kwargs):
        sns.boxplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    def plot_scatter(data, clusters, **kwargs):
        sns.swarmplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    if len(frame) > 0:
        for (file, plot_fn) in (
                ("result_boxplot", plot_box),
                ("result_scatterplot", plot_scatter)
        ):
            plot = create_plot(frame, plot_fn)
            plot.savefig(os.path.join(directory, f"{file}.png"))
