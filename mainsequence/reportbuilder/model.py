from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Union

from pydantic import BaseModel, Field, HttpUrl, root_validator, validator

# ────────────────────────────── Common enums ──────────────────────────────


class HorizontalAlign(str, Enum):
    left = "left"
    center = "center"
    right = "right"


class VerticalAlign(str, Enum):
    top = "top"
    center = "center"
    bottom = "bottom"


class FontWeight(str, Enum):
    normal = "normal"
    bold = "bold"


class Anchor(str, Enum):
    top_left = "top_left"
    top_right = "top_right"
    bottom_left = "bottom_left"
    bottom_right = "bottom_right"
    center = "center"


class Size(BaseModel):
    width: str | None = None  # "300px", "40%", …
    height: str | None = None

    @validator("width", "height", pre=True)
    def _coerce_to_str(cls, v):  # noqa: N805
        if v is None:
            return v
        return f"{v}px" if isinstance(v, int) else str(v)

    def css(self) -> str:
        return "".join(
            f"{dim}:{val};"
            for dim, val in (("width", self.width), ("height", self.height))
            if val is not None
        )


class Position(BaseModel):
    top: str | None = None
    left: str | None = None
    right: str | None = None
    bottom: str | None = None
    anchor: Anchor | None = None

    @validator("top", "left", "right", "bottom", pre=True)
    def _coerce_to_str(cls, v):
        if v is None:
            return v
        return f"{v}px" if isinstance(v, int) else str(v)

    def css(self) -> str:
        if self.anchor and any([self.top, self.left, self.right, self.bottom]):
            raise ValueError("Specify either 'anchor' or explicit offsets - not both")
        if self.anchor:
            return {
                Anchor.top_left: "top:0;left:0;",
                Anchor.top_right: "top:0;right:0;",
                Anchor.bottom_left: "bottom:0;left:0;",
                Anchor.bottom_right: "bottom:0;right:0;",
                Anchor.center: "top:50%;left:50%;transform:translate(-50%,-50%);",
            }[self.anchor]

        return "".join(
            f"{side}:{val};"
            for side, val in (
                ("top", self.top),
                ("left", self.left),
                ("right", self.right),
                ("bottom", self.bottom),
            )
            if val is not None
        )


class ElementBase(BaseModel):
    id: str = Field(default_factory=lambda: f"elem_{id(object())}")
    z_index: int = 1
    css_class: str | None = None

    class Config:
        arbitrary_types_allowed = True

    def render(self) -> str:
        raise NotImplementedError


class TextElement(ElementBase):
    text: str

    # 1) semantic element type
    element_type: Literal["h1", "h2", "h3", "h4", "h5", "h6", "p"] = "p"

    font_weight: FontWeight = FontWeight.normal
    h_align: HorizontalAlign = HorizontalAlign.left
    v_align: VerticalAlign = VerticalAlign.top
    color: str | None = None
    line_height: str | None = None
    size: Size = Field(default_factory=Size)
    position: Position | None = None

    style_theme: ThemeMode | None = None

    def render(self, override_theme_mode_if_none) -> str:
        if self.style_theme is None:
            self.style_theme = override_theme_mode_if_none

        settings = self.style_theme
        style = []

        if self.position:
            style.append("position:absolute;")
            style.append(self.position.css())
        style.append(self.size.css())

        # choose font-size and default color
        if self.element_type.startswith("h"):
            ff = settings.font_family_headings
            default_color = settings.heading_color
        else:
            ff = settings.font_family_paragraphs
            default_color = settings.paragraph_color

        # use explicit color if given, else default
        c = self.color if self.color is not None else default_color

        style.append(f"font-weight:{self.font_weight.value};")
        style.append(f"font-family:{ff};")
        style.append(f"color:{c};")
        style.append(f"text-align:{self.h_align.value};")

        if self.line_height:
            style.append(f"line-height:{self.line_height};")

        class_attr = f'class="text-element-{self.element_type}"' if self.element_type else ""
        tag = self.element_type

        return (
            f'<{tag} id="{self.id}" {class_attr} ' f'style="{"".join(style)}">{self.text}</{tag}>'
        )


class TextH1(TextElement):
    # force element_type to always be "h1" and disallow overrides
    element_type: Literal["h1"] = Field("h1", literal=True)

    def __init__(self, **data):
        # Remove any attempt to pass element_type in data
        data.pop("element_type", None)
        super().__init__(**data)


class TextH2(TextElement):
    # force element_type to always be "h1" and disallow overrides
    element_type: Literal["h2"] = Field("h2", literal=True)

    def __init__(self, **data):
        # Remove any attempt to pass element_type in data
        data.pop("element_type", None)
        super().__init__(**data)


class ImageElement(ElementBase):
    src: str
    alt: str = ""
    size: Size = Field(default_factory=lambda: Size(width="100%", height="auto"))
    position: Position | None = None
    object_fit: str = "contain"
    style_theme: ThemeMode | None = None

    def render(self, override_theme_mode_if_none) -> str:
        if self.style_theme is None:
            self.style_theme = override_theme_mode_if_none
        style = []
        if self.position:
            style.append("position:absolute;")
            style.append(self.position.css())
        style.append(self.size.css())
        style.append(f"object-fit:{self.object_fit};")

        class_attr = f'class="{self.css_class}"' if self.css_class else ""
        return (
            f'<img id="{self.id}" {class_attr} src="{self.src}" alt="{self.alt}" '
            f'style="{"".join(style)}" crossOrigin="anonymous" />'
        )


class HtmlElement(ElementBase):
    html: str
    style_theme: ThemeMode | None = None

    def render(self, override_theme_mode_if_none) -> str:
        if self.style_theme is None:
            self.style_theme = override_theme_mode_if_none
        class_attr = f'class="{self.css_class}"' if self.css_class else ""
        return f'<div id="{self.id}" {class_attr}">{self.html}</div>'


BaseElements = Union[TextElement, ImageElement, HtmlElement]


class GridCell(BaseModel):
    row: int
    col: int
    row_span: int = 1
    col_span: int = 1
    element: BaseElements
    padding: str | None = None
    background_color: str | None = None
    align_self: str | None = None
    justify_self: str | None = None
    content_v_align: VerticalAlign | None = (
        None  # For vertical alignment of content WITHIN the cell
    )
    content_h_align: HorizontalAlign | None = (
        None  # For horizontal alignment of content WITHIN the cell
    )

    @validator("row", "col", "row_span", "col_span", pre=True)
    def _positive(cls, v):
        if isinstance(v, str) and v.isdigit():
            v = int(v)
        if not isinstance(v, int) or v < 1:
            raise ValueError("row/col/row_span/col_span must be positive integers >= 1")
        return v


class GridLayout(BaseModel):
    row_definitions: list[str] = Field(default_factory=lambda: ["1fr"])
    col_definitions: list[str] = Field(default_factory=lambda: ["1fr"])
    gap: int = 10
    cells: list[GridCell]
    width: str | None = "100%"
    height: str | None = "100%"
    style_theme: ThemeMode | None = None

    @validator("gap", pre=True)
    def _coerce_gap_to_int(cls, v):
        if isinstance(v, str) and v.endswith("px"):
            return int(v[:-2])
        if isinstance(v, str) and v.isdigit():
            return int(v)
        if isinstance(v, int):
            return v
        raise ValueError("gap must be an int or string like '10px'")

    @validator("cells", each_item=True)
    def _within_grid(cls, cell: GridCell, values: dict[str, Any]) -> GridCell:
        row_defs = values.get("row_definitions")
        col_defs = values.get("col_definitions")
        if row_defs and cell.row + cell.row_span - 1 > len(row_defs):
            raise ValueError(
                f"GridCell definition (row={cell.row}, row_span={cell.row_span}) exceeds row count ({len(row_defs)})"
            )
        if col_defs and cell.col + cell.col_span - 1 > len(col_defs):
            raise ValueError(
                f"GridCell definition (col={cell.col}, col_span={cell.col_span}) exceeds column count ({len(col_defs)})"
            )
        return cell

    def render(
        self,
    ) -> str:
        grid_style_parts = [
            "display:grid;",
            f"grid-template-columns:{' '.join(self.col_definitions)};",
            f"grid-template-rows:{' '.join(self.row_definitions)};",
            f"gap:{self.gap}px;",
            "position:relative;",
        ]
        if self.width:
            grid_style_parts.append(f"width:{self.width};")
        if self.height:
            grid_style_parts.append(f"height:{self.height};")
        grid_style = "".join(grid_style_parts)

        html_parts: list[str] = [f'<div class="slide-grid" style="{grid_style}">']
        for cell in self.cells:
            cell_styles_list = [
                f"grid-column:{cell.col}/span {cell.col_span};",
                f"grid-row:{cell.row}/span {cell.row_span};",
                "position:relative;",
                "display:flex;",
            ]

            align_items_css_value = "flex-start"
            if cell.content_v_align:
                if cell.content_v_align == VerticalAlign.center:
                    align_items_css_value = "center"
                elif cell.content_v_align == VerticalAlign.bottom:
                    align_items_css_value = "flex-end"
                elif cell.content_v_align == VerticalAlign.top:
                    align_items_css_value = "flex-start"
            elif isinstance(cell.element, TextElement) and cell.element.v_align:
                if cell.element.v_align == VerticalAlign.center:
                    align_items_css_value = "center"
                elif cell.element.v_align == VerticalAlign.bottom:
                    align_items_css_value = "flex-end"

            justify_content_css_value = "flex-start"
            if cell.content_h_align:
                if cell.content_h_align == HorizontalAlign.center:
                    justify_content_css_value = "center"
                elif cell.content_h_align == HorizontalAlign.right:
                    justify_content_css_value = "flex-end"
                elif cell.content_h_align == HorizontalAlign.left:
                    justify_content_css_value = "flex-start"
            elif isinstance(cell.element, TextElement) and cell.element.h_align:
                if cell.element.h_align == HorizontalAlign.center:
                    justify_content_css_value = "center"
                elif cell.element.h_align == HorizontalAlign.right:
                    justify_content_css_value = "flex-end"

            cell_styles_list.append(f"align-items: {align_items_css_value};")
            cell_styles_list.append(f"justify-content: {justify_content_css_value};")

            if cell.padding:
                cell_styles_list.append(f"padding:{cell.padding};")
            if cell.background_color:
                cell_styles_list.append(f"background-color:{cell.background_color};")

            if cell.align_self:
                cell_styles_list.append(f"align-self:{cell.align_self};")
            if cell.justify_self:
                cell_styles_list.append(f"justify-self:{cell.justify_self};")

            final_cell_style = "".join(cell_styles_list)
            try:
                html_parts.append(
                    f'<div style="{final_cell_style}">{cell.element.render(self.style_theme)}</div>'
                )
            except Exception as e:
                raise e
        html_parts.append("</div>")
        return "".join(html_parts)


class Slide(BaseModel):
    title: str
    layout: GridLayout
    notes: str | None = None
    include_logo_in_header: bool = True
    footer_info: str = ""
    body_margin_top: int = 5

    style_theme: StyleSettings | None = None

    def _section_style(self) -> str:
        # only background color; size determined by container
        return f"background-color:{self.style_theme.background_color};"

    def _render_header(self) -> str:
        title_class = "text-element-h2"
        title_inline_style = f"color: {self.style_theme.heading_color};"  # Only color here

        logo_html = self.style_theme.logo_img_html() if self.include_logo_in_header else ""
        return (
            f'<div class="slide-header">'
            f'  <div class="slide-title {title_class} fw-bold" style="{title_inline_style}">'
            f"{self.title}</div>"
            f"{logo_html}"
            f"</div>"
        )

    def _render_body(self) -> str:
        style = (
            f"flex:1; display:flex; flex-direction:column;" f" margin-top:{self.body_margin_top}px;"
        )
        return f'<div class="slide-body" style="{style}">' f"{self.layout.render()}" f"</div>"

    def _render_footer(
        self,
        slide_number: int,
        total: int,
    ) -> str:
        text_style = f"color: {self.style_theme.light_paragraph_color};"
        return (
            f'<div class="slide-footer">'
            f'<div class="slide-date" style="{text_style}">{self.footer_info}</div>'
            f'<div class="slide-number" style="{text_style}">{slide_number} / {total}</div>'
            f"</div>"
        )

    def _override_theme(self, theme_mode: ThemeMode):
        if self.style_theme is None:
            self.style_theme = theme_mode
        if self.layout.style_theme is None:
            self.layout.style_theme = theme_mode

    def render(self, slide_number: int, total: int, override_theme_mode_if_none: ThemeMode) -> str:
        self._override_theme(override_theme_mode_if_none)
        header = self._render_header()
        body = self._render_body()
        footer = self._render_footer(
            slide_number,
            total,
        )
        section_style = self._section_style()

        return (
            f'<section class="slide" style="{section_style}">'
            f"{header}{body}{footer}"
            f"</section>"
        )


class VerticalImageSlide(Slide):
    image_url: HttpUrl = Field(..., description="URL for the right-column image")
    image_width_pct: int = Field(
        50, ge=0, le=100, description="Percentage width of the right-column image"
    )
    image_fit: Literal["cover", "contain"] = Field(
        "cover", description="How the image should fit its container"
    )

    def render(self, slide_number: int, total: int, override_theme_mode_if_none: ThemeMode) -> str:
        self._override_theme(override_theme_mode_if_none)
        header = self._render_header()
        body = self._render_body()
        footer = self._render_footer(slide_number, total)

        # Determine inline widths
        left_pct = 100 - self.image_width_pct
        left_style = f"width:{left_pct}%;"
        right_style = f"width:{self.image_width_pct}%; padding:0;"
        img_style = f"width:100%; height:100%; object-fit:{self.image_fit};"

        # Compose columns
        left_html = f'<div class="left-column" style="{left_style}">' f"{body}" f"</div>"
        right_html = (
            f'<div class="right-column" style="{right_style}">'
            f'  <img src="{self.image_url}" alt="" style="{img_style}" />'
            f"</div>"
        )

        # Section tag uses both classes and background style
        section_style = self._section_style()
        return (
            f'<section class="slide vertical-image-slide" style="{section_style}">'
            f"{left_html}{right_html}"
            f"</section>"
        )


class ThemeMode(str, Enum):
    light = "light"
    dark = "dark"


class StyleSettings(BaseModel):
    """
    Pydantic model for theme-based style settings.
    Provides a semantic typographic scale (h1–h6, p), separate font families for headings and paragraphs,
    and chart palettes. Colors and palettes are auto-filled based on `mode`.
    """

    # theme switch
    mode: ThemeMode = ThemeMode.light

    # semantic typographic scale
    font_size_h1: int = 32
    font_size_h2: int = 28
    font_size_h3: int = 24
    font_size_h4: int = 20
    font_size_h5: int = 16
    font_size_h6: int = 14
    font_size_p: int = 12

    # default font families
    font_family_headings: str = "Montserrat, sans-serif"
    font_family_paragraphs: str = "Lato, Arial, Helvetica, sans-serif"

    # layout
    title_column_width: str = "150px"
    chart_label_font_size: int = 12
    logo_url: str | None = None

    # theme-driven colors (auto-filled)
    primary_color: str | None = Field(None)
    secondary_color: str | None = Field(None)
    accent_color_1: str | None = Field(None)
    accent_color_2: str | None = Field(None)
    heading_color: str | None = Field(None)
    paragraph_color: str | None = Field(None)
    background_color: str | None = Field(None)
    light_paragraph_color: str | None = Field(
        None, description="Paragraph text color on light backgrounds"
    )

    # chart color palettes
    chart_palette_sequential: list[str] | None = Field(None)
    chart_palette_diverging: list[str] | None = Field(None)
    chart_palette_categorical: list[str] | None = Field(None)

    def logo_img_html(self, position: str = "slide-logo") -> str:
        return (
            f'<div class="{position}"><img src="{self.logo_url}" alt="logo" crossOrigin="anonymous"></div>'
            if self.logo_url
            else ""
        )

    @root_validator(pre=True)
    def _fill_theme_defaults(cls, values: dict) -> dict:
        palettes = {
            ThemeMode.light: {
                # base colors
                "primary_color": "#c0d8fb",
                "secondary_color": "#1254ff",
                "accent_color_1": "#553ffe",
                "accent_color_2": "#aea06c",
                "heading_color": "#c0d8fb",
                "paragraph_color": "#303238",
                "background_color": "#FFFFFF",
                "light_paragraph_color": "#303238",
                # chart palettes
                "chart_palette_sequential": ["#f7fbff", "#deebf7", "#9ecae1", "#3182bd"],
                "chart_palette_diverging": ["#d7191c", "#fdae61", "#ffffbf", "#abdda4", "#2b83ba"],
                "chart_palette_categorical": [
                    "#1b9e77",
                    "#d95f02",
                    "#7570b3",
                    "#e7298a",
                    "#66a61e",
                ],
            },
            ThemeMode.dark: {
                "primary_color": "#E0E0E0",  # light gray for primary text
                "secondary_color": "#BB86FC",  # soft purple accent
                "accent_color_1": "#03DAC6",  # vibrant teal
                "accent_color_2": "#CF6679",  # warm pink/red
                "heading_color": "#FFFFFF",  # pure white for headings
                "paragraph_color": "#E0E0E0",  # slightly muted white for body text
                "background_color": "#121212",  # deep charcoal
                "light_paragraph_color": "#E0E0E0",
                "chart_palette_sequential": [
                    "#37474F",  # slate blue-gray
                    "#455A64",
                    "#546E7A",
                    "#607D8B",  # progressively lighter
                    "#78909C",
                ],
                "chart_palette_diverging": [
                    "#D32F2F",  # strong red
                    "#F57C00",  # orange
                    "#EEEEEE",  # near-white neutral mid-point
                    "#0288D1",  # bright blue
                    "#1976D2",  # deeper blue
                ],
                "chart_palette_categorical": [
                    "#F94144",  # red
                    "#F3722C",  # orange
                    "#F9C74F",  # yellow
                    "#90BE6D",  # green
                    "#577590",  # indigo
                    "#43AA8B",  # teal
                    "#8E44AD",  # purple
                ],
            },
        }
        mode = values.get("mode", ThemeMode.light)
        for field, default in palettes.get(mode, {}).items():
            values.setdefault(field, default)
        return values


# ─── instantiate both themes ────────────────────────────────────────────
light_settings: StyleSettings = StyleSettings(mode=ThemeMode.light)
dark_settings: StyleSettings = StyleSettings(mode=ThemeMode.dark)


def get_theme_settings(mode: ThemeMode) -> StyleSettings:
    """
    Retrieve the global light or dark settings instance.
    """
    return light_settings if mode is ThemeMode.light else dark_settings


def update_settings_from_dict(overrides: dict, mode: ThemeMode) -> None:
    """
    Update either `light_settings` or `dark_settings` in-place from dict `overrides`.

    - `overrides` may include any fields (colors, fonts, layout, palettes).
    - `mode` selects which settings instance to modify.
    """
    # select global instance
    instance = get_theme_settings(mode)
    # merge current values with overrides
    merged = instance.dict()
    merged.update(overrides)
    # create a temporary instance to re-apply root defaults and validation
    temp = StyleSettings(**merged)
    # mutate the existing settings instance so imports remain valid
    for key, value in temp.dict().items():
        setattr(instance, key, value)


class Presentation(BaseModel):
    title: str
    slides: list[Slide]
    style_theme: ThemeMode = Field(default_factory=lambda: light_settings)

    def render(self) -> str:
        slides_html = []

        # add the slide template
        # self.slides.append(self._slide_template())
        for slide in self.slides:
            if slide.style_theme is None:
                slide.style_theme = self.style_theme

        total = len(self.slides) - 1  # do not add the final template slide

        slides_html += [
            s.render(i + 1, total, override_theme_mode_if_none=s.style_theme)
            for i, s in enumerate(self.slides)
        ]
        return BASE_TEMPLATE.render(
            title=self.title,
            font_family=self.style_theme.font_family_paragraphs,
            slides="".join(slides_html),
        )

    def _slide_template(self) -> Slide:

        # 1) Four rows:
        #    - First row “auto” for our split tutorial
        #    - Then the three demo rows (100px, 2fr, 1fr)
        row_definitions = ["auto", "100px", "2fr", "1fr"]

        # 2) Twelve columns mixing px and fr
        col_definitions = [
            "50px",
            "1fr",
            "2fr",
            "100px",
            "3fr",
            "1fr",
            "200px",
            "2fr",
            "1fr",
            "150px",
            "4fr",
            "1fr",
        ]

        # 3) Tutorial cells in row 1:
        cells: list[GridCell] = [
            # Left tutorial text (cols 1–6) with detailed fr explanation
            GridCell(
                row=1,
                col=1,
                col_span=6,
                element=TextElement(
                    text=(
                        "<strong>Tutorial: How fr Units Are Calculated</strong><br><br>"
                        "1. <em>Start with total container height</em> (e.g. 800px).<br>"
                        "2. <em>Subtract auto/fixed rows</em>:<br>"
                        "   • Row 1 (auto) → measured by content, say 200px<br>"
                        "   • Row 2 (fixed) → exactly 100px<br>"
                        "   → Used: 300px<br>"
                        "3. <em>Free space</em> = 800px − 300px = 500px<br>"
                        "4. <em>Total fr shares</em> = 2fr + 1fr = 3 shares<br>"
                        "5. <em>One share</em> = 500px ÷ 3 ≈ 166.67px<br>"
                        "6. <em>Allocate</em>:<br>"
                        "   • Row 3 (2fr) → 2×166.67px ≈ 333.33px<br>"
                        "   • Row 4 (1fr) → 1×166.67px ≈ 166.67px<br><br>"
                        "→ That’s how 2fr can take twice the free space and still leave one share for 1fr!"
                    ),
                    font_size=14,
                    h_align=HorizontalAlign.left,
                    v_align=VerticalAlign.top,
                ),
                padding="12px",
                background_color="#f9f9f9",
            ),
            # Right tutorial code (cols 7–12)
            GridCell(
                row=1,
                col=7,
                col_span=6,
                element=TextElement(
                    text=(
                        '<pre style="font-size:12px; white-space:pre-wrap;">'
                        "row_defs = ['auto', '100px', '2fr', '1fr']\n"
                        "col_defs = ['50px','1fr','2fr','100px','3fr','1fr',\n"
                        "             '200px','2fr','1fr','150px','4fr','1fr']\n\n"
                        "slide = GridLayout(\n"
                        "    row_definitions=row_defs,\n"
                        "    col_definitions=col_defs,\n"
                        "    gap='10px',\n"
                        "    cells=...  # see demo rows below\n"
                        ")\n"
                        "</pre>"
                    ),
                    font_size=12,
                    h_align=HorizontalAlign.left,
                    v_align=VerticalAlign.top,
                ),
                padding="12px",
                background_color="#ffffff",
            ),
        ]

        # 4) Demo cells for rows 2–4
        for r in range(2, 5):  # rows 2, 3, 4
            for c in range(1, 13):  # cols 1–12
                label = f"R{r}({row_definitions[r - 1]}), C{c}({col_definitions[c - 1]})"
                cells.append(
                    GridCell(
                        row=r,
                        col=c,
                        element=TextElement(
                            text=label,
                            font_size=12,
                            h_align=HorizontalAlign.center,
                            v_align=VerticalAlign.center,
                        ),
                    )
                )

        # 5) Build and render the layout
        slide_layout = GridLayout(
            row_definitions=row_definitions,
            col_definitions=col_definitions,
            gap="10px",
            cells=cells,
            width="100%",
            height="100%",
        )

        return Slide(
            title="Slide Template",
            layout=slide_layout,
        )
