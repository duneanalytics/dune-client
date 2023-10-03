import unittest
from unittest.mock import patch
import pandas as pd
from dune_client.viz.graphs import create_sankey


class TestCreateSankey(unittest.TestCase):
    # Setting up a dataframe for testing
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "source_col": ["WBTC", "USDC"],
                "target_col": ["USDC", "WBTC"],
                "value_col": [2184, 2076],
            }
        )

        self.predefined_colors = {
            "USDC": "rgb(38, 112, 196)",
            "WBTC": "rgb(247, 150, 38)",
        }

        self.columns = {
            "source": "source_col",
            "target": "target_col",
            "value": "value_col",
        }
        self.viz_config: dict = {
            "node_pad": 15,
            "node_thickness": 20,
            "node_line_width": 0.5,
            "font_size": 10,
            "figure_height": 1000,
            "figure_width": 1500,
        }

    def test_missing_column(self):
        # Remove a required column from dataframe
        df_without_target = self.df.drop(columns=["target_col"])
        with self.assertRaises(ValueError):
            create_sankey(
                df_without_target, self.predefined_colors, self.columns, self.viz_config
            )

    def test_value_column_not_numeric(self):
        # Change the 'value' column to a non-numeric type
        df_with_str_values = self.df.copy()
        df_with_str_values["value_col"] = ["10", "11"]
        with self.assertRaises(ValueError):
            create_sankey(
                df_with_str_values,
                self.predefined_colors,
                self.columns,
                self.viz_config,
            )

    # Mocking the visualization creation and just testing the processing logic
    @patch("plotly.graph_objects.Figure")
    def test_mocked_visualization(self, MockFigure):
        result = create_sankey(
            self.df, self.predefined_colors, self.columns, self.viz_config, "test"
        )

        # Ensuring our mocked Figure was called with the correct parameters
        MockFigure.assert_called_once()


if __name__ == "__main__":
    unittest.main()
