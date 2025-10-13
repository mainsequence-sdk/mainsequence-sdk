import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, conint, constr

from .base import BaseObjectOrm, BasePydanticModel
from .utils import make_request

# Define a reusable HexColor annotated type
HexColor = Annotated[
    str,
    Field(
        pattern=r"^#[0-9A-Fa-f]{6}$",
        description="HEX color in format #RRGGBB",
        min_length=7,
        max_length=7,
    ),
]


class Theme(BasePydanticModel, BaseObjectOrm):
    id: int
    theme_type: constr(max_length=15) = Field(..., description="‘standard’ or ‘custom’")
    name: constr(max_length=100)
    created: datetime.datetime
    updated: datetime.datetime

    mode: constr(max_length=5) = Field(..., description="‘light’ or ‘dark’")
    editor_background: dict | None = Field(None, description="FK to Background.id")

    font_family_headings: constr(max_length=100) | None = Field(
        "", description="--font-family-headings"
    )
    font_family_paragraphs: constr(max_length=100) | None = Field(
        "", description="--font-family-paragraphs"
    )

    font_size_h1: conint(ge=1) = Field(48, description="--font-size-h1 (px)")
    font_size_h2: conint(ge=1) = Field(40, description="--font-size-h2 (px)")
    font_size_h3: conint(ge=1) = Field(32, description="--font-size-h3 (px)")
    font_size_h4: conint(ge=1) = Field(24, description="--font-size-h4 (px)")
    font_size_h5: conint(ge=1) = Field(20, description="--font-size-h5 (px)")
    font_size_h6: conint(ge=1) = Field(16, description="--font-size-h6 (px)")
    font_size_p: conint(ge=1) = Field(14, description="--font-size-p  (px)")

    primary_color: HexColor = Field("#0d6efd")
    secondary_color: HexColor = Field("#6c757d")
    accent_color_1: HexColor = Field("#198754")
    accent_color_2: HexColor = Field("#ffc107")
    heading_color: HexColor = Field("#212529")
    paragraph_color: HexColor = Field("#495057")
    light_paragraph_color: HexColor = Field("#6c757d")
    background_color: HexColor = Field("#ffffff")

    title_column_width: constr(max_length=15) = Field("150px", description="--title-column-width")
    chart_label_font_size: conint(ge=1) = Field(12, description="--chart-label-font-size (px)")

    chart_palette_sequential: list[HexColor] = Field(
        default_factory=list,
        description="List of 5 HEX colours for sequential palettes",
    )
    chart_palette_diverging: list[HexColor] = Field(
        default_factory=list,
        description="List of 5 HEX colours for diverging palettes",
    )
    chart_palette_categorical: list[HexColor] = Field(
        default_factory=list,
        description="List of 6 HEX colours for categorical palettes",
    )

    def set_plotly_theme(self):
        import plotly.graph_objects as go
        import plotly.io as pio

        try:
            import plotly.express as px
        except ImportError:
            px = None

        # --------------------- derive colours and fonts --------------------------
        base_font = dict(
            family=self.font_family_paragraphs or "Inter, Arial, sans-serif",
            size=self.font_size_p,
            color=self.paragraph_color,
        )
        title_font = dict(
            family=self.font_family_headings or base_font["family"],
            size=self.font_size_h4,
            color=self.heading_color,
        )

        # categorical palette
        colorway = (
            self.chart_palette_categorical[:]
            if len(self.chart_palette_categorical) >= 3
            else [
                self.primary_color,
                self.secondary_color,
                self.accent_color_1,
                self.accent_color_2,
                self.heading_color,
                self.paragraph_color,
            ]
        )

        sequential = (
            self.chart_palette_sequential[:]
            if len(self.chart_palette_sequential) >= 3
            else colorway
        )
        diverging = (
            self.chart_palette_diverging[:]
            if len(self.chart_palette_diverging) >= 3
            else colorway[::-1]
        )

        paper_bg = plot_bg = self.background_color
        if self.mode.lower() == "dark" and paper_bg.lower() == "#ffffff":
            paper_bg = plot_bg = "#111111"

        # --------------------------- build template ------------------------------
        template = go.layout.Template(
            layout=dict(
                font=base_font,
                title_font=title_font,
                paper_bgcolor=paper_bg,
                plot_bgcolor=plot_bg,
                colorway=colorway,
                #  ↓↓↓  **correct spot for continuous scales**  ↓↓↓
                colorscale=dict(
                    sequential=sequential,
                    diverging=diverging,
                ),
                xaxis=dict(showgrid=False, zeroline=False),
                yaxis=dict(showgrid=True, zeroline=False),
            )
        )

        # ---------------------- register & activate ------------------------------
        tpl_name = f"theme_{self.id}_{self.name}"
        pio.templates[tpl_name] = template
        pio.templates.default = tpl_name
        if px:
            px.defaults.template = tpl_name
            px.defaults.color_discrete_sequence = colorway
            px.defaults.color_continuous_scale = sequential


class Folder(BasePydanticModel, BaseObjectOrm):
    id: int | None = None
    name: str
    slug: str

    @classmethod
    def get_or_create(cls, *args, **kwargs):
        url = f"{cls.get_object_url()}/get-or-create/"
        payload = {"json": kwargs}
        r = make_request(
            s=cls.build_session(), loaders=cls.LOADERS, r_type="POST", url=url, payload=payload
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error appending creating: {r.text}")
        # Return a new instance of AssetCategory built from the response JSON.
        return cls(**r.json())


class Slide(BasePydanticModel, BaseObjectOrm):
    id: int | None = None
    number: int = Field(..., ge=0, description="Number of the slide in presentation order")
    body: str = Field(
        ...,
        description=(
            "Tiptap rich-text document for the main content, serialized as JSON. "
            "Must follow the Tiptap ‘doc’ schema, e.g.: "
            '{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}'
        ),
        example='{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}',
    )
    header: str | None = Field(
        None,
        description=(
            "Optional Tiptap rich-text document for the header, serialized as JSON. "
            "If present, should follow the Tiptap ‘doc’ schema, e.g.: "
            '{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}'
        ),
        example='{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}',
    )
    footer: str | None = Field(
        None,
        description=(
            "Optional Tiptap rich-text document for the footer, serialized as JSON. "
            "If present, should follow the Tiptap ‘doc’ schema, e.g.: "
            '{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}'
        ),
        example='{"type":"doc","content":[{"type":"paragraph","attrs":{"textAlign":null}}]}',
    )
    created_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow,
        description="Timestamp when the record was created",
    )
    updated_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow,
        description="Timestamp when the record was last updated",
    )
    custom_format: dict | None = Field(
        description="Extra configuration for different type of slides"
    )


class Presentation(BasePydanticModel, BaseObjectOrm):
    id: int
    folder: int
    title: str = Field(..., max_length=255)
    description: str | None = Field("", description="Optional text")
    created_at: datetime.datetime
    updated_at: datetime.datetime
    theme: int | Theme
    slides: list[Slide]

    @classmethod
    def get_or_create_by_title(cls, *args, **kwargs):
        url = f"{cls.get_object_url()}/get_or_create_by_title/"
        payload = {"json": kwargs}
        r = make_request(
            s=cls.build_session(), loaders=cls.LOADERS, r_type="POST", url=url, payload=payload
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error appending creating: {r.text}")
        # Return a new instance of AssetCategory built from the response JSON.
        return cls(**r.json())

    def add_slide(self, *args, **kwargs) -> Slide:
        url = f"{self.get_object_url()}/{self.id}/add_slide/"

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": {}},
        )
        if r.status_code not in [201]:
            raise Exception(f"Error appending creating: {r.text}")
        # Return a new instance of AssetCategory built from the response JSON.
        return Slide(**r.json())


##########


class TextNode(BaseModel):
    type: Literal["text"] = Field(..., description="Inline text node")
    text: str = Field(..., description="Text content")


class TextParagraphAttrs(BaseModel):
    textAlign: Literal["left", "right", "center", "justify"] | None = Field(
        None, description="Text alignment within the block"
    )
    level: int | None = Field(
        None, ge=1, le=6, description="Heading level (1–6); only set when `type` == 'heading'"
    )


class TextParagraph(BaseModel):
    """
    Tiptap 'paragraph' or 'heading' block with inline text content.
    """

    type: Literal["paragraph", "heading"] = Field(
        ..., description="Block type: 'paragraph' or 'heading'"
    )
    attrs: TextParagraphAttrs = Field(
        default_factory=TextParagraphAttrs,
        description="Block attributes (alignment and optional heading level)",
    )
    content: list[TextNode] = Field(..., description="Inline text nodes")

    @classmethod
    def paragraph(
        cls, text: str, text_align: Literal["left", "right", "center", "justify"] | None = None
    ) -> "TextParagraph":
        """
        Build a simple paragraph block:

            TextParagraph.paragraph("Hello world", text_align="center")
        """
        return cls(
            type="paragraph",
            attrs=TextParagraphAttrs(textAlign=text_align),
            content=[TextNode(type="text", text=text)],
        )

    @classmethod
    def heading(
        cls,
        text: str,
        level: int = 1,
        text_align: Literal["left", "right", "center", "justify"] | None = None,
    ) -> "TextParagraph":
        """
        Build a heading block of the given level (1–6):

            TextParagraph.heading("Title", level=2, text_align="center")
        """
        return cls(
            type="heading",
            attrs=TextParagraphAttrs(textAlign=text_align, level=level),
            content=[TextNode(type="text", text=text)],
        )

    class Config:
        json_schema_extra = {
            "examples": {
                "paragraph": {
                    "summary": "Basic paragraph",
                    "value": {
                        "type": "paragraph",
                        "attrs": {"textAlign": None},
                        "content": [{"type": "text", "text": "This is a body paragraph."}],
                    },
                },
                "heading": {
                    "summary": "Centered H1 heading",
                    "value": {
                        "type": "heading",
                        "attrs": {"textAlign": "center", "level": 1},
                        "content": [{"type": "text", "text": "This is a heading"}],
                    },
                },
            }
        }


### App Nodes
class EndpointProps(BaseModel):
    props: dict[str, int] = Field(
        ...,
        description="Dictionary of props to send to the endpoint, e.g. {'id': 33}",
        example={"id": 33},
    )
    url: str = Field(
        ...,
        description="Relative or absolute URL for the API endpoint",
        example="/orm/api/reports/run-function/",
    )


class AppNodeAttrs(BaseModel):
    endpointProps: EndpointProps = Field(..., description="Configuration for the endpoint call")


class AppNode(BaseModel):
    """
    Represents a custom Tiptap node of type 'appNode' that invokes an API.
    """

    type: Literal["appNode"] = Field("appNode", description="Node type identifier")
    attrs: AppNodeAttrs = Field(..., description="Node attributes")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "appNode",
                "attrs": {
                    "endpointProps": {"props": {"id": 33}, "url": "/orm/api/reports/run-function/"}
                },
            }
        }

    @classmethod
    def make_app_node(cls, id: int, url: str = "/orm/api/reports/run-function/") -> "AppNode":
        return cls(attrs=AppNodeAttrs(endpointProps=EndpointProps(props={"id": id}, url=url)))
