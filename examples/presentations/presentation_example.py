import json

import mainsequence.client as ms_client

# Placeholder variables for the example
PRESENTATION_TITLE = "My Portfolio Factor Analysis"
FOLDER_NAME = "Factor Analysis Reports"
THEME_NAME = "Main Sequence"
ARTIFACT_ID_FOR_CHART = "artifact_abc123"  # A placeholder artifact ID for an uploaded chart

# --- Main Script ---

# Get the folder where the presentation will be stored
# The get_or_create method is useful to avoid creating duplicate folders
folder = ms_client.Folder.get_or_create(name=FOLDER_NAME)

# Step 1: Create or retrieve the Presentation object
# get_or_create_by_title finds an existing presentation with the same title
# in the specified folder or creates a new one.
presentation = ms_client.Presentation.get_or_create_by_title(
    title=PRESENTATION_TITLE,
    folder=folder.id,
    description=f"Automatically generated presentation for {PRESENTATION_TITLE}",
)
print(f"Successfully retrieved or created Presentation with ID: {presentation.id}")

# Step 2: Retrieve and apply a Theme
# Themes control the visual style (colors, fonts) of the presentation.
try:
    theme = ms_client.Theme.get(name=THEME_NAME)
    presentation.patch(theme_id=theme.id)
    # This applies the theme's Plotly template to ensure charts are consistent
    presentation.theme.set_plotly_theme()
    print(f"Applied theme '{THEME_NAME}' to the presentation.")
except ms_client.client.ResourceNotFound:
    print(f"Theme '{THEME_NAME}' not found. Using default theme.")

# Step 3: Add and populate a Slide
# You can add slides sequentially. Here, we add the first slide (index 0).

# A) Add a new, empty slide to the presentation
# The add_slide() method appends a new slide and returns it.
new_slide = presentation.add_slide()
print(f"Added a new slide with ID: {new_slide.id}")

# B) Define the content for the slide
# The body of a slide is a JSON object. We create it using helper models.
# It can contain headings, paragraphs, and embedded app nodes (like charts).
slide_body_content = {
    "type": "doc",
    "content": [
        # Add a main title for the slide
        ms_client.TextParagraph.heading("Portfolio Exposure", level=2).model_dump(),
        # Embed the chart using its artifact ID
        json.loads(ms_client.AppNode.make_app_node(id=ARTIFACT_ID_FOR_CHART).model_dump_json()),
        # Add a descriptive note at the bottom
        ms_client.TextParagraph.paragraph(
            "Exposures calculated from 2025-07-01 to 2025-07-31"
        ).model_dump(),
    ],
}

# C) Update the slide with the defined content
# The patch() method updates the slide's properties, like its body.
new_slide.patch(body=json.dumps(slide_body_content))
print(f"Successfully populated slide {new_slide.id} with a title, chart, and note.")

# To add another slide, you would simply repeat Step 3.
# For example, to add a second slide:
# slide_two = presentation.add_slide()
# ... define content for slide_two ...
# slide_two.patch(body=json.dumps(content_for_slide_two))
