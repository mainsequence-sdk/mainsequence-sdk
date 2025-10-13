import tempfile
from pathlib import Path

from nbconvert import PythonExporter


def convert_notebook_to_python_file(notebook_path):
    """
    Converts a Jupyter notebook to a Python file in a temporary directory.

    Args:
        notebook_path (str or pathlib.Path): The path to the Jupyter notebook (.ipynb) file.

    Returns:
        pathlib.Path: The path to the generated Python file in the temporary directory.
    """

    # Ensure notebook_path is a Path object
    notebook_path = Path(notebook_path)

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Convert the notebook to a Python script
    exporter = PythonExporter()
    (python_code, resources) = exporter.from_filename(str(notebook_path))

    # Create the output file path
    python_file_path = Path(temp_dir) / (notebook_path.stem + ".py")

    # Write the Python code to the file
    with open(python_file_path, "w", encoding="utf-8") as f:
        f.write(python_code)

    print(f"Notebook converted and saved to: {python_file_path}")

    return python_file_path
