# Instrument Forms

The `mainsequence.dashboards.streamlit.instruments` package is built around one important idea:

instrument configuration forms should come from the same Pydantic models that define the configuration itself.

That is what `StreamlitModelFormFactory` does.

## Why this exists

Instrument and pricing configuration models are usually too rich to maintain comfortably as hand-written Streamlit forms.

Typical problems with hand-built forms:

- labels drift away from model names
- validation rules get duplicated
- nested config models become tedious
- schedule editing becomes messy
- QuantLib-specific fields need custom UI

The form factory solves that by reading the model schema and rendering a Streamlit UI from it.

## Main helpers

### `StreamlitModelFormFactory`

This is the main entry point.

It reads a Pydantic model and renders inputs for:

- primitive fields
- enums
- optional fields
- nested models
- lists of scalars
- lists of models
- dates
- numeric fields with constraints
- schema-driven display metadata such as titles, descriptions, and examples

It also understands extra schema hints that are useful for finance and QuantLib-style objects.

### `RenderResult`

The factory returns a `RenderResult` with:

- `raw_values`: the raw values collected from the UI
- `instance`: the built Pydantic instance, when validation succeeds
- `errors`: validation errors, when the model could not be built

This is a good contract for dashboards because it lets you show validation feedback before persisting or pricing anything.

### `schedule_editor(...)`

This helper renders an explicit schedule editor for QuantLib-style schedule inputs.

It supports:

- dates
- calendar
- business day convention
- termination business day convention
- end-of-month flag
- tenor
- schedule rule

That is exactly the sort of form that becomes fragile and repetitive if every dashboard builds it from scratch.

## QuantLib-aware behavior

One of the most useful parts of this helper layer is that it recognizes domain-specific schema hints.

The form factory can treat fields differently when they represent:

- `Schedule`
- `Calendar`
- `DayCounter`
- `Period`
- `BusinessDayConvention`

It also respects schema metadata such as:

- titles
- descriptions
- examples
- minimum/maximum constraints
- `json_schema_extra` hints
- UI hints like percent formatting and scaling

That makes the rendered form much closer to the real intent of the model.

## Grid editing and fallback behavior

For editable lists and schedules, the helper can use `st_aggrid` when it is available.

If `st_aggrid` is not installed, it falls back to built-in Streamlit editors such as:

- `st.data_editor`
- `st.text_area`

That is a practical design choice. It gives you a better editing experience when the extra dependency is present, but it does not make the whole form system unusable without it.

## Example

```python
import streamlit as st

from mainsequence.dashboards.streamlit.instruments.streamlit_form_factory import (
    StreamlitModelFormFactory,
)

factory = StreamlitModelFormFactory()
result = factory.render(
    MyInstrumentConfig,
    key_prefix="bond.",
    initial=None,
    build_instance=True,
)

if result.errors:
    st.write(result.errors)
else:
    config = result.instance
```

This is a good fit when your dashboard is collecting or editing rich instrument configuration and you want the model itself to remain the source of truth.

## Why we recommend this approach

We recommend using these helpers for instrument dashboards because they are already aligned with the kinds of models used in the Main Sequence platform.

That gives you three concrete advantages:

- less duplicated validation logic
- less custom UI code for finance-specific fields
- fewer surprises when the dashboard data needs to flow back into platform objects or pricing logic

If your dashboard works with instrument definitions, pricing parameters, or schedule-heavy models, this helper package should be your first option rather than your fallback.
