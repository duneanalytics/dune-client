"""
Functions you can call to make different graphs
"""

import plotly.graph_objects as go
import colorlover as cl
import pandas as pd

# function to create Sankey diagram
def create_sankey(
    query_result: pd.DataFrame,
    predefined_colors: {},
    source: str = "source",
    target: str = "target",
    value: str = "value",
    title: str = "unnamed",
    node_pad: int = 15,
    node_thickness: int = 20,
    node_line_width: int = 0.5,
    font_size: int = 10,
    figure_height: int = 1000,
    figure_width: int = 1500,
):
    """
    Creates a Sankey diagram based on input query_result
    , which must contain source, target, value columns
    """
    # Check if the dataframe contains required columns
    required_columns = [source, target, value]
    for col in required_columns:
        if col not in query_result.columns:
            raise ValueError(f"Error: The dataframe is missing the '{col}' column")
        
    # Check if 'value' column is numeric
    if not pd.api.types.is_numeric_dtype(query_result[value]):
        raise ValueError("Error: The 'value' column must be numeric")

    # preprocess query result dataframe
    all_nodes = list(pd.concat([query_result[source], query_result[target]]).unique())
    # In Sankey, 'source' and 'target' must be indices. Thus, you need to map projects to indices.
    query_result["source_idx"] = query_result[source].map(all_nodes.index)
    query_result["target_idx"] = query_result[target].map(all_nodes.index)

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
                pad=node_pad,
                thickness=node_thickness,
                line=dict(color="black", width=node_line_width),
                label=all_nodes,
                color=[
                    color_map.get(node, "blue") for node in all_nodes
                ],  # customize node color
            ),
            link=dict(
                source=query_result["source_idx"],
                target=query_result["target_idx"],
                value=query_result[value],
                color=[
                    color_map.get(query_result[source].iloc[i], "black")
                    for i in range(len(query_result))
                ],  # customize link color
            ),
        )
    )
    fig.update_layout(
        title_text=title, font_size=font_size, height=figure_height, width=figure_width
    )

    return fig
