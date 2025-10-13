from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pydantic import BaseModel, Field

import mainsequence.client as ms_client
from mainsequence.reportbuilder.slide_templates import plot_dataframe_line_chart
from mainsequence.virtualfundbuilder.resource_factory.app_factory import BaseApp, register_app
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class ExampleAppConfiguration(BaseModel):
    age: int = Field(27, description="User age")
    name: str = Field("Daniel Garber", description="User name")


@register_app()
class ExampleApp(BaseApp):
    configuration_class = ExampleAppConfiguration

    def run(self) -> None:
        logger.info(
            f"This tool is been used by {self.configuration.name} "
            f"whom is {self.configuration.age}"
        )
        df = pd.DataFrame(
            {
                "Sales 2025": [150, 200, 250, 300, 400],
                "Sales 2024": [120, 180, 220, 260, 310],
            },
            index=pd.Index(["Jan", "Feb", "Mar", "Apr", "May"], name="month"),
        )
        plotly_html = plot_dataframe_line_chart(df)
        with TemporaryDirectory() as tmpdir:
            output_html_path = Path(tmpdir) / "chart.html"
            output_html_path.write_text(plotly_html, encoding="utf-8")

            # Do the upload while the temp dir still exists
            html_artifact = ms_client.Artifact.upload_file(
                filepath=str(output_html_path),
                name="test_valuer",
                created_by_resource_name=self.__class__.__name__,
                bucket_name="Test Bucket",
            )
        self.add_output(html_artifact)


if __name__ == "__main__":
    configuration = ExampleAppConfiguration()
    ExampleApp(configuration).run()
