"""
Functions you can call to make different graphs
"""

import plotly.graph_objects as go
import colorlover as cl
import pandas as pd

# function to create Sankey diagram
def create_sankey(
    query_result: pd.DataFrame,
    predefined_colors: dict,
    columns: dict,
    viz_config: dict,
    title: str = "unnamed",
):
    """
    Creates a Sankey diagram based on input query_result
    , which must contain source, target, value columns
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
            node=dict(
                pad=viz_config["node_pad"],
                thickness=viz_config["node_thickness"],
                line=dict(color="black", width=viz_config["node_line_width"]),
                label=all_nodes,
                color=[
                    color_map.get(node, "blue") for node in all_nodes
                ],  # customize node color
            ),
            link=dict(
                source=query_result["source_idx"],
                target=query_result["target_idx"],
                value=query_result[columns["value"]],
                color=[
                    color_map.get(query_result[columns["source"]].iloc[i], "black")
                    for i in range(len(query_result))
                ],  # customize link color
            ),
        )
    )
    fig.update_layout(
        title_text=title,
        font_size=viz_config["font_size"],
        height=viz_config["figure_height"],
        width=viz_config["figure_width"],
    )

    return fig
