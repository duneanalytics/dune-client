"""
Functions you can call to make different graphs
"""

from typing import Dict, Union

# https://github.com/plotly/colorlover/issues/35
import colorlover as cl  # type: ignore[import-untyped]
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
from plotly.graph_objs import Figure  # type: ignore[import-untyped]


# function to create Sankey diagram
def create_sankey(
    query_result: pd.DataFrame,
    predefined_colors: Dict[str, str],
    columns: Dict[str, str],
    viz_config: Dict[str, Union[int, float]],
    title: str = "unnamed",
) -> Figure:
    """
    Creates a Sankey diagram based on input query_result,
    which must contain source, target, value columns.
    Column names don't have to be exact same but there must be
    these three columns conceptually and value column must be numeric.
    """
    # Check if the dataframe contains required columns
    required_columns = [columns["source"], columns["target"], columns["value"]]
    for col in required_columns:
        if col not in query_result.columns:
            raise ValueError(f"Error: The dataframe is missing the '{col}' column")

    # Check if 'value' column is numeric
    if not pd.api.types.is_numeric_dtype(query_result[columns["value"]]):
        raise ValueError("Error: The 'value' column must be numeric")

    # preprocess query result dataframe
    all_nodes = list(
        pd.concat(
            [query_result[columns["source"]], query_result[columns["target"]]]
        ).unique()
    )
    # In Sankey, 'source' and 'target' must be indices. Thus, you need to map projects to indices.
    query_result["source_idx"] = query_result[columns["source"]].map(all_nodes.index)
    query_result["target_idx"] = query_result[columns["target"]].map(all_nodes.index)

    # create color map for Sankey
    colors = cl.scales["12"]["qual"]["Set3"]  # default color
    color_map = {}
    for node in all_nodes:
        for name, color in predefined_colors.items():
            if name.lower() in node.lower():  # check if name exists in the node name
                color_map[node] = color
                break
        else:
            color_map[node] = colors[
                len(color_map) % len(colors)
            ]  # default color assignment

    fig = go.Figure(
        go.Sankey(
            node={
                "pad": viz_config["node_pad"],
                "thickness": viz_config["node_thickness"],
                "line": {"color": "black", "width": viz_config["node_line_width"]},
                "label": all_nodes,
                "color": [
                    color_map.get(node, "blue") for node in all_nodes
                ],  # customize node color
            },
            link={
                "source": query_result["source_idx"],
                "target": query_result["target_idx"],
                "value": query_result[columns["value"]],
                "color": [
                    color_map.get(query_result[columns["source"]].iloc[i], "black")
                    for i in range(len(query_result))
                ],  # customize link color
            },
        )
    )
    fig.update_layout(
        title_text=title,
        font_size=viz_config["font_size"],
        height=viz_config["figure_height"],
        width=viz_config["figure_width"],
    )

    return fig
