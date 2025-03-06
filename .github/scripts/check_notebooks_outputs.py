"""
Script to check if a Jupyter notebook has uncleared outputs.
"""

import json
import sys
from io import TextIOWrapper

import nbformat


def check_notebook_outputs(notebook_stream: TextIOWrapper, notebook_path: str) -> None:
    """
    Check if a Jupyter notebook has uncleared outputs.

    Args:
        notebook_stream (TextIOWrapper): A text stream of the contents of the modified Jupyter notebook.
        notebook_path (str): The relative path to the notebook file in the repository.
    """
    # Read the notebook content from the stream
    notebook_content = notebook_stream.read()

    # Ensure the file is valid JSON
    try:
        json.loads(notebook_content)
    except json.JSONDecodeError as e:
        print(
            f"Warning: Notebook '{notebook_path}' does not appear to be valid JSON. Skipping. Error: {e}"
        )
        return  # Skip further checks for this file

    # Parse the notebook using nbformat
    try:
        notebook = nbformat.reads(notebook_content, as_version=4)
    except Exception as e:
        print(
            f"Warning: Notebook '{notebook_path}' could not be parsed by nbformat. Skipping. Error: {e}"
        )
        return  # Skip further checks for this file

    # Check for uncleared outputs
    has_output = False
    for cell in notebook.cells:
        if cell.cell_type == "code" and "outputs" in cell:
            if cell.outputs:
                has_output = True
                print(
                    f"Notebook '{notebook_path}' has outputs that need to be cleared."
                )
                break

    if has_output:
        print(
            f"Notebook '{notebook_path}' has outputs that need to be cleared. Run "
            f"`jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace {notebook_path}`"
        )


if __name__ == "__main__":
    main_notebook_path = sys.argv[1]
    check_notebook_outputs(sys.stdin, main_notebook_path)
