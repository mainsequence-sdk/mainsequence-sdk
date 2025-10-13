import os


def build_markdown_index():
    # Fixed source directory and target file location
    base_dir = "../examples"
    target_file = "../docs/examples/index.md"

    # Ensure the output directory exists

    # Markdown introduction text
    intro = (
        "# Examples\n\n"
        "This repository contains a collection of independent Jupyter notebooks designed to clearly illustrate the functionality of the Main Sequence SDK. "
        "Each notebook demonstrates a specific use case, providing practical insights into the SDK's capabilities.\n\n"
        "You can explore the full set of examples here:  \n"
        "[Main Sequence SDK Examples Repository](https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/examples)\n\n"
        "Below are categorized examples for easier navigation.\n"
    )

    markdown_lines = [intro, ""]

    # Walk through the directory tree of the fixed base directory
    for root, dirs, files in os.walk(base_dir):
        # Ensure consistent ordering
        dirs.sort()
        files.sort()

        # Compute relative directory from base_dir
        rel_dir = os.path.relpath(root, base_dir)
        # Create a header based on the directory depth
        if rel_dir == ".":
            header = "# Root"
        else:
            header_level = (
                rel_dir.count(os.sep) + 2
            )  # h2 for first level, h3 for second level, etc.
            header = f"{'#' * header_level} {os.path.basename(root)}"
        markdown_lines.append(header)
        markdown_lines.append("")  # Blank line for spacing

        # Find all Jupyter notebooks (.ipynb) in the current directory
        notebooks = [f for f in files if f.endswith(".ipynb")]
        for nb in notebooks:
            # Build a relative path link for the notebook (using forward slashes for URLs)
            nb_path = os.path.join(rel_dir, nb) if rel_dir != "." else nb
            nb_path = nb_path.replace("\\", "/")
            markdown_lines.append(f"- [{nb}]({nb_path})")

        markdown_lines.append("")  # Empty line after each directory section

    # Write the generated markdown to the target file
    with open(target_file, "w", encoding="utf-8") as f:
        f.write("\n".join(markdown_lines))

    print(f"Markdown index generated at: {target_file}")


if __name__ == "__main__":
    build_markdown_index()
