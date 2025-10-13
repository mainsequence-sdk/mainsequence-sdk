import hashlib
import json
import os
from abc import abstractmethod
from typing import Any

from pydantic import BaseModel

from mainsequence.client.models_tdag import Artifact, add_created_object_to_jobrun
from mainsequence.virtualfundbuilder.enums import ResourceType
from mainsequence.virtualfundbuilder.resource_factory.base_factory import (
    BaseResource,
    insert_in_registry,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class BaseAgentTool(BaseResource):
    TYPE = ResourceType.APP

    def __init__(self, configuration: BaseModel):
        self.configuration = configuration

    def add_output(self, output: Any):
        """
        Saves the given output in the backend.
        """
        logger.info(f"Add object {output} to job run output")
        job_id = os.getenv("JOB_RUN_ID", None)

        if job_id:
            add_created_object_to_jobrun(
                model_name=output.orm_class, app_label=output.get_app_label(), object_id=output.id
            )
            logger.info("Output added successfully")
        else:
            logger.info("This is not a Job Run - no output can be added")

    @staticmethod
    def hash_pydantic_object(obj: Any, digest_size: int = 16) -> str:
        """
        Generate a unique SHA-256 hash for any Pydantic object (including nested dependencies),
        ensuring that lists of objects are deterministically ordered.

        Args:
            obj: A Pydantic BaseModel instance or any JSON-serializable structure.

        Returns:
            A hex string representing the SHA-256 hash of the canonical JSON representation.
        """

        def serialize(item: Any) -> Any:
            if isinstance(item, BaseModel):
                return serialize(item.dict(by_alias=True, exclude_unset=True))
            elif isinstance(item, dict):
                return {str(k): serialize(v) for k, v in sorted(item.items())}
            elif isinstance(item, (list, tuple, set)):
                serialized_items = [serialize(v) for v in item]
                try:
                    return sorted(
                        serialized_items,
                        key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":")),
                    )
                except (TypeError, ValueError):
                    return serialized_items
            else:
                if hasattr(item, "isoformat"):
                    try:
                        return item.isoformat()
                    except Exception:
                        pass
                return item

        json_str = json.dumps(serialize(obj), sort_keys=True, separators=(",", ":"))
        h = hashlib.blake2b(digest_size=digest_size)
        h.update(json_str.encode("utf-8"))
        return h.hexdigest()


AGENT_TOOL_REGISTRY = AGENT_TOOL_REGISTRY if "AGENT_TOOL_REGISTRY" in globals() else {}


def regiester_agent_tool(name=None, register_in_agent=True):
    """
    Decorator to register a model class in the factory.
    If `name` is not provided, the class's name is used as the key.
    """

    def decorator(cls):
        return insert_in_registry(AGENT_TOOL_REGISTRY, cls, register_in_agent, name)

    return decorator


class HtmlApp(BaseAgentTool):
    """
    A base class for apps that generate HTML output.
    """

    TYPE = ResourceType.HTML_APP

    def __init__(self, *args, **kwargs):
        self.created_artifacts = []
        super().__init__(*args, **kwargs)

    def _get_hash_from_configuration(self):
        try:
            return hashlib.sha256(
                json.dumps(self.configuration.__dict__, sort_keys=True, default=str).encode()
            ).hexdigest()[:8]
        except Exception as e:
            logger.warning(f"[{self.__name__}] Could not hash configuration: {e}")

    def add_html_output(self, html_content, output_name=None):
        """
        Saves the given HTML content to a file, uploads it as an artifact,
        and stores the artifact reference.
        If output_name is not provided, a sequential name (e.g., ClassName_1.html) is generated.
        """
        if not isinstance(html_content, str):
            raise TypeError(
                f"The 'add_html_output' method of {self.__class__.__name__} must be called with a string of HTML content."
            )

        if output_name is None:
            output_name = len(self.created_artifacts)

        output_name = f"{self.__class__.__name__}_{output_name}.html"

        try:
            with open(output_name, "w", encoding="utf-8") as f:
                f.write(html_content)

            logger.info(f"[{self.__class__.__name__}] Successfully saved HTML to: {output_name}")
        except OSError as e:
            logger.error(f"[{self.__class__.__name__}] Error saving file: {e}")
            raise

        job_id = os.getenv("JOB_ID", None)
        if job_id:
            html_artifact = None
            try:
                html_artifact = Artifact.upload_file(
                    filepath=output_name,
                    name=output_name,
                    created_by_resource_name=self.__class__.__name__,
                    bucket_name="HTMLOutput",
                )
                if html_artifact:
                    self.created_artifacts.append(html_artifact)
                    self.add_output(html_artifact)
                    logger.info(f"Artifact uploaded successfully: {html_artifact.id}")
                else:
                    logger.info("Artifact upload failed")
            except Exception as e:
                logger.info(f"Error uploading artifact: {e}")

    def __init_subclass__(cls, **kwargs):
        """
        Wraps the subclass's `run` method to add validation and saving logic.
        """
        super().__init_subclass__(**kwargs)
        original_run = cls.run

        def run_wrapper(self, *args, **kwargs) -> str:
            html_content = original_run(self, *args, **kwargs)

            if html_content:
                self.add_html_output(html_content)

        cls.run = run_wrapper

    @abstractmethod
    def run(self) -> str:
        """
        This method should be implemented by subclasses to return HTML content as a string.
        The base class will handle saving the output.
        """
        raise NotImplementedError("Subclasses of HtmlApp must implement the 'run' method.")
