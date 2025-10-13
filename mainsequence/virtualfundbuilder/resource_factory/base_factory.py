import ast
import importlib.util
import inspect
import os
import sys
from enum import Enum
from pathlib import Path
from typing import Union, get_type_hints

from pydantic import BaseModel

from mainsequence.client.models_tdag import DynamicResource
from mainsequence.tdag import DataNode
from mainsequence.virtualfundbuilder.utils import create_schema_from_signature, get_vfb_logger

logger = get_vfb_logger()
from mainsequence.virtualfundbuilder.utils import runs_in_main_process


class BaseResource:
    @classmethod
    def get_source_notebook(cls):
        """Retrieve the exact source code of the class from notebook cells."""
        from IPython import get_ipython

        ipython_shell = get_ipython()
        history = ipython_shell.history_manager.get_range()

        for _, _, cell_content in history:
            try:
                # Parse the cell content as Python code
                parsed = ast.parse(cell_content)

                # Look for the class definition in the AST (Abstract Syntax Tree)
                for node in ast.walk(parsed):
                    if isinstance(node, ast.ClassDef) and node.name == cls.__name__:
                        # Extract the start and end lines of the class
                        start_line = node.lineno - 1
                        end_line = max(
                            [child.lineno for child in ast.walk(node) if hasattr(child, "lineno")]
                        )
                        lines = cell_content.splitlines()
                        return "\n".join(lines[start_line:end_line])
            except Exception as e:
                print(e)
                continue

        return "Class definition not found in notebook history."

    @classmethod
    def build_and_parse_from_configuration(cls, **kwargs) -> "WeightsBase":
        type_hints = get_type_hints(cls.__init__)

        def parse_value_into_hint(value, hint):
            """
            Recursively parse `value` according to `hint`.
            Handles:
              - Pydantic models
              - Enums
              - Lists of Pydantic models
              - Optional[...] / Union[..., NoneType]
            """
            if value is None:
                return None

            from typing import get_args, get_origin

            origin = get_origin(hint)
            args = get_args(hint)

            # Handle Optional/Union
            # e.g. Optional[SomeModel] => Union[SomeModel, NoneType]
            if origin is Union and len(args) == 2 and type(None) in args:
                # Identify the non-None type
                non_none_type = args[0] if args[1] == type(None) else args[1]
                return parse_value_into_hint(value, non_none_type)

            # Handle single Pydantic model
            if inspect.isclass(hint) and issubclass(hint, BaseModel):
                if not isinstance(value, hint):
                    return hint(**value)
                return value

            # Handle single Enum
            if inspect.isclass(hint) and issubclass(hint, Enum):
                if not isinstance(value, hint):
                    return hint(value)
                return value

            # Handle List[...] of Pydantic models or other types
            if origin is list:
                inner_type = args[0]
                # If the list elements are Pydantic models
                if inspect.isclass(inner_type) and issubclass(inner_type, BaseModel):
                    return [
                        inner_type(**item) if not isinstance(item, inner_type) else item
                        for item in value
                    ]
                # If the list elements are Enums, or other known transformations, handle similarly
                if inspect.isclass(inner_type) and issubclass(inner_type, Enum):
                    return [
                        inner_type(item) if not isinstance(item, inner_type) else item
                        for item in value
                    ]
                # Otherwise, just return the list as is
                return value

            # If none of the above, just return the value unchanged.
            return value

        # Now loop through each argument in kwargs and parse
        for arg, value in kwargs.items():
            if arg in type_hints:
                hint = type_hints[arg]
                kwargs[arg] = parse_value_into_hint(value, hint)

        return cls(**kwargs)


SKIP_REGISTRATION = os.getenv("SKIP_REGISTRATION", "").lower() == "true"


def insert_in_registry(registry, cls, register_in_agent, name=None, attributes: dict | None = None):
    """helper for strategy decorators"""
    key = name or cls.__name__  # Use the given name or the class name as the key

    if key in registry and register_in_agent:
        logger.debug(f"{cls.TYPE} '{key}' is already registered.")
        return cls

    registry[key] = cls
    logger.debug(f"Registered {cls.TYPE} class '{key}': {cls}")

    if register_in_agent and not SKIP_REGISTRATION and runs_in_main_process():
        send_resource_to_backend(cls, attributes)
        # Thread(
        #     target=send_resource_to_backend,
        #     args=(cls, attributes),
        # ).start()

    return cls


class BaseFactory:
    @staticmethod
    def import_module(strategy_name):
        VFB_PROJECT_PATH = os.environ.get("VFB_PROJECT_PATH", None)
        assert VFB_PROJECT_PATH, "There is no signals folder variable specified"

        project_path = Path(VFB_PROJECT_PATH)

        strategy_folder_path = project_path / strategy_name
        logger.debug(f"Registering signals from {strategy_folder_path}")
        package_name = f"{project_path.name}.{strategy_name}"

        project_root_path = project_path.parent.parent
        if project_root_path not in sys.path:
            sys.path.insert(0, project_root_path)

        for filename in os.listdir(strategy_folder_path):
            try:
                if filename.endswith(".py"):
                    # Build the full module name
                    module_name = f"{package_name}.{filename[:-3]}"

                    # Dynamically import the module
                    module = importlib.import_module(module_name)
            except Exception as e:
                logger.warning(f"Error reading code in strategy {filename}: {e}")


def send_resource_to_backend(resource_class, attributes: dict | None = None):
    """
    Parses the __init__ signatures of the class and its parents to generate a
    unified JSON schema and sends the resource payload to the backend.
    """
    merged_properties = {}
    merged_required = set()
    merged_definitions = {}

    # Special case for BaseAgentTool subclasses that use a configuration_class
    if (
        hasattr(resource_class, "configuration_class")
        and inspect.isclass(resource_class.configuration_class)
        and issubclass(resource_class.configuration_class, BaseModel)
    ):
        config_class = resource_class.configuration_class
        config_name = config_class.__name__

        # Get the full schema for the configuration class
        config_schema = config_class.model_json_schema(ref_template="#/$defs/{model}")

        # Merge any nested definitions from the config schema
        if "$defs" in config_schema:
            merged_definitions.update(config_schema.pop("$defs"))

        # Add the configuration class's own schema to the definitions
        merged_definitions[config_name] = config_schema

        # Create a top-level "configuration" property that references the schema
        merged_properties["configuration"] = {
            "$ref": f"#/$defs/{config_name}",
            "title": "Configuration",
        }
        # Mark the top-level "configuration" as required
        merged_required.add("configuration")

    else:
        # Standard logic for other resource types
        for parent_class in reversed(resource_class.__mro__):
            if (
                parent_class is object
                or not hasattr(parent_class, "__init__")
                or parent_class is DataNode
            ):
                continue
            if "__init__" in parent_class.__dict__:
                parent_schema = create_schema_from_signature(parent_class.__init__)
                merged_properties.update(parent_schema.get("properties", {}))
                merged_definitions.update(parent_schema.get("$defs", {}))
                merged_required.update(parent_schema.get("required", []))

    final_json_schema = {
        "title": resource_class.__name__,
        "type": "object",
        "properties": merged_properties,
    }
    if merged_required:
        schema_required = sorted(
            [
                field
                for field in merged_required
                if "default" not in merged_properties.get(field, {})
            ]
        )
        if schema_required:
            final_json_schema["required"] = schema_required

    if merged_definitions:
        final_json_schema["$defs"] = merged_definitions

    resource_config = DynamicResource.create(
        name=resource_class.__name__,
        type=resource_class.TYPE.value,
        object_signature=final_json_schema,
        attributes=attributes,
    )

    logger.debug(f"Sending resource '{resource_class.__name__}' to backend.")
