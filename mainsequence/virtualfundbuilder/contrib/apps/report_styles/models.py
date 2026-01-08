from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, root_validator


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
