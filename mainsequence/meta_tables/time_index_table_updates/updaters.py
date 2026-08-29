from __future__ import annotations

import datetime
import inspect
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import pandas as pd

from mainsequence.client.metatables import (
    BaseUpdateStatistics,
    Scheduler,
    SessionDataSource,
    TimeIndexTableUpdate,
    UpdateStatistics,
)
from mainsequence.code_repository_context import require_code_repository_branch_context
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.meta_tables import PlatformTimeIndexMetaTable
from mainsequence.meta_tables.time_index_table_refs import (
    TimeIndexTableAccessMixin,
    TimeIndexTableRef,
)

from . import configuration, runner
from .configuration import BaseConfiguration
from .managers import (
    TimeIndexTableUpdateManager,
    ensure_registered_output_table,
)
from .namespacing import current_hash_namespace
from .namespacing import hash_namespace as _hash_namespace_cm

if TYPE_CHECKING:
    from .dependencies import TableDependency

LocalUpdateResult = None | pd.DataFrame | Sequence[Any]


class TimeIndexTableUpdater(TimeIndexTableAccessMixin, ABC):
    """
    Base class for building and maintaining datasets in Main Sequence.

    A ``TimeIndexTableUpdater`` is the update process recipe:

    - configuration for update behavior,
    - dependencies,
    - update logic.

    Two identities matter:

    - ``output_table``: the first-class PlatformTimeIndexMetaTable storage contract.
    - ``update_hash``: identifies the updater job writing to that table.

    This separation lets you run different updater jobs (for example, asset shards)
    while writing into the same table safely.

    Hash namespace
    --------------
    ``TimeIndexTableUpdater`` also supports ``hash_namespace`` for test and experiment isolation.

    Resolution order is:

    1. explicit ``hash_namespace="..."`` passed at construction,
    2. subclass-declared aliases such as ``namespace="..."``,
    3. the active ``with hash_namespace("...")`` context manager,
    4. otherwise, no namespace.

    A non-empty namespace is injected into the build configuration, which changes
    ``update_hash``. Storage identity stays on the explicit ``output_table``.

    During ``run()``, the active namespace is re-applied around the full run so
    dependencies created inside ``dependencies()`` inherit the same isolation.

    Subclass checklist:

    - Keep constructor args stable and serializable (Pydantic config is recommended).
    - Build dependencies in ``__init__`` and return them in ``dependencies()``.
    - Use ``self.update_statistics`` in ``update()`` and return only incremental rows.
    - Provide table/column metadata for production datasets.
    """

    OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC)
    dependency_kind: ClassVar[Literal["table_update"]] = "table_update"
    TABLE_UPDATE_CLASS = TimeIndexTableUpdate

    def __init__(
        self,
        config: BaseConfiguration,
        output_table: type[PlatformTimeIndexMetaTable],
        *,
        hash_namespace: str | None = None,
    ):
        """
        Initialize framework-level state for the updater.

        Preferred pattern:
        subclasses build a concrete ``TimeIndexTableUpdateConfig`` and pass it as
        ``super().__init__(config=..., output_table=...)``.

        The initial fallback start date for first-run updates is ``OFFSET_START``.

        Parameters
        ----------
        config : BaseConfiguration
            Canonical configuration for this updater.
        output_table : type[PlatformTimeIndexMetaTable]
            Explicit time-indexed platform storage model where this update
            process writes. This is runtime state, not part of the build
            configuration payload.
        hash_namespace : str | None
            Optional hash isolation namespace.
        """
        if not isinstance(config, BaseConfiguration):
            raise TypeError(
                f"{self.__class__.__name__} expected config to be a BaseConfiguration; "
                f"got {type(config).__name__}."
            )
        if not issubclass(output_table, PlatformTimeIndexMetaTable):
            raise TypeError(
                f"{self.__class__.__name__} expected output_table to be a subclass of "
                f"PlatformTimeIndexMetaTable; got {output_table}."
            )

        self.pre_load_routines_run = False
        self._update_manager: TimeIndexTableUpdateManager | None = None
        self.output_table = output_table

        self._scheduler_tree_connected = False
        self.update_statistics = None
        self.config = config
        self._framework_initialized = True

        if hash_namespace is not None:
            self._hash_namespace = (hash_namespace or "").strip()

    def __init_subclass__(cls, **kwargs):
        """
        Wrap subclass construction so TimeIndexTableUpdater can capture config, compute hashes,
        and apply namespace/test controls consistently.

        The wrapper consumes ``hash_namespace`` before the subclass ``__init__`` runs:

        - ``hash_namespace="..."``: explicit hash isolation namespace

        Namespace precedence is:

        1. explicit ``hash_namespace``
        2. subclass-declared aliases such as ``namespace``
        3. active namespacing context manager

        Only a non-empty namespace is injected into the hashed build configuration.
        """
        super().__init_subclass__(**kwargs)

        # Get the original __init__ from the new subclass
        original_init = cls.__init__

        @wraps(original_init)
        def wrapped_init(self, *args, **kwargs):
            # ---- hashing namespace controls (never forwarded to user __init__) ----
            if "test_node" in kwargs:
                raise TypeError(
                    "test_node has been removed from TimeIndexTableUpdater construction; use "
                    'hash_namespace="..." or with hash_namespace("...") instead.'
                )
            explicit_namespace = kwargs.pop("hash_namespace", None)
            namespace_aliases = tuple(getattr(cls, "_HASH_NAMESPACE_ALIASES", ()) or ())
            provided_namespace_aliases = [alias for alias in namespace_aliases if alias in kwargs]
            alias_namespace = None
            alias_namespace_provided = bool(provided_namespace_aliases)
            if len(provided_namespace_aliases) > 1:
                raise ValueError(
                    f"{cls.__name__} received multiple hash namespace aliases: "
                    f"{provided_namespace_aliases!r}."
                )
            if alias_namespace_provided:
                alias_namespace = kwargs.pop(provided_namespace_aliases[0])
            if (
                explicit_namespace is not None
                and alias_namespace_provided
                and (explicit_namespace or "").strip() != (alias_namespace or "").strip()
            ):
                raise ValueError(
                    f"{cls.__name__} received both hash_namespace and "
                    f"{provided_namespace_aliases[0]} with different values."
                )
            if "init_meta" in kwargs:
                raise TypeError(
                    "init_meta has been removed from TimeIndexTableUpdater construction; remove this keyword argument."
                )

            # Determine namespace:
            # 1) explicit hash_namespace kwarg wins
            # 2) subclass-declared alias kwarg, such as namespace
            # 3) else: context manager namespace
            if explicit_namespace is not None:
                namespace = explicit_namespace
            elif alias_namespace_provided:
                namespace = alias_namespace
            else:
                namespace = current_hash_namespace()

            namespace = (namespace or "").strip()
            # 1. Call the original __init__ of the subclass first
            if namespace:
                with _hash_namespace_cm(namespace):
                    original_init(self, *args, **kwargs)
            else:
                original_init(self, *args, **kwargs)

            if not getattr(self, "_framework_initialized", False):
                raise TypeError(
                    f"{self.__class__.__name__} must call super().__init__(config=..., "
                    "output_table=..., hash_namespace=...) from its constructor."
                )
            if not isinstance(getattr(self, "config", None), BaseConfiguration):
                raise TypeError(
                    f"{self.__class__.__name__} must pass a BaseConfiguration to "
                    "super().__init__(config=..., output_table=...)."
                )

            # 2. Capture all arguments from __init__ methods in the MRO up to TimeIndexTableUpdater
            final_kwargs = {}
            mro = self.__class__.mro()

            try:
                # We want to inspect from parent to child to ensure subclass arguments override.
                # The MRO is ordered from child to parent, so we find TimeIndexTableUpdater and reverse the part before it.
                updater_index = mro.index(TimeIndexTableUpdater)
                classes_to_inspect = reversed(mro[:updater_index])
            except ValueError:
                # Fallback if TimeIndexTableUpdater is not in the MRO.
                classes_to_inspect = [self.__class__]

            def _bind_supported_arguments(sig: inspect.Signature) -> inspect.BoundArguments:
                remaining_args = list(args)
                positional_args: list[Any] = []
                consumed_positionally: set[str] = set()
                accepts_var_keyword = False

                for param in sig.parameters.values():
                    if param.name == "self":
                        continue

                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        if remaining_args:
                            positional_args.append(remaining_args.pop(0))
                            consumed_positionally.add(param.name)
                    elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                        positional_args.extend(remaining_args)
                        remaining_args.clear()
                    elif param.kind == inspect.Parameter.VAR_KEYWORD:
                        accepts_var_keyword = True

                filtered_kwargs: dict[str, Any] = {}
                extra_kwargs: dict[str, Any] = {}

                for key, value in kwargs.items():
                    param = sig.parameters.get(key)

                    if param is None:
                        if accepts_var_keyword:
                            extra_kwargs[key] = value
                        continue

                    if key in consumed_positionally:
                        continue

                    if param.kind in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    ):
                        filtered_kwargs[key] = value
                    elif param.kind == inspect.Parameter.VAR_KEYWORD:
                        extra_kwargs[key] = value

                if accepts_var_keyword:
                    filtered_kwargs.update(extra_kwargs)

                return sig.bind_partial(self, *positional_args, **filtered_kwargs)

            for cls_to_inspect in classes_to_inspect:
                # Only inspect the __init__ defined on the class itself.
                if "__init__" in cls_to_inspect.__dict__:
                    sig = inspect.signature(cls_to_inspect.__init__)
                    try:
                        bound_args = _bind_supported_arguments(sig)
                        bound_args.apply_defaults()

                        current_args = dict(bound_args.arguments)
                        current_args.pop("self", None)

                        for param in sig.parameters.values():
                            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                                current_args.pop(param.name, None)
                            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                                final_kwargs.update(current_args.pop(param.name, {}))

                        # Update the final arguments. Overwrites parent args with child args.
                        final_kwargs.update(current_args)
                    except TypeError as exc:
                        logger.warning(
                            f"Could not bind filtered arguments for "
                            f"{cls_to_inspect.__name__}.__init__; skipping for config. "
                            f"Error: {exc}"
                        )
                        continue

            # Remove `args` as it collects un-named positional arguments which are not part of the config hash.
            final_kwargs.pop("args", None)
            for namespace_alias in namespace_aliases:
                final_kwargs.pop(namespace_alias, None)
            output_table = final_kwargs.pop("output_table", None)
            if output_table is not None:
                self.output_table = output_table
            if final_kwargs.get("config") is None:
                final_kwargs.pop("config", None)
            if not final_kwargs:
                final_kwargs["config"] = self.config

            self._hash_namespace = namespace
            if self._hash_namespace:
                final_kwargs["hash_namespace"] = self._hash_namespace
                logger.debug(f"Running on namespace {self._hash_namespace}")
            # 3. Run the post-initialization routines
            self.build_configuration = final_kwargs
            logger.debug(f"Running post-init routines for {self.__class__.__name__}")
            self._initialize_configuration(init_kwargs=final_kwargs)

            logger.bind(update_hash=self.update_hash)

            self.run_after_post_init_routines()

            # requirements for graph update
            self.dependencies_df: pd.DataFrame | None = None
            self.depth_df: pd.DataFrame | None = None

            self.scheduler: Scheduler | None = None
            self.update_details_tree: dict[str, Any] | None = None

            logger.debug(f"Post-init routines for {self.__class__.__name__} complete.")

        # Replace the subclass's __init__ with our new wrapped version
        cls.__init__ = wrapped_init

    @property
    def output_table(self) -> type[PlatformTimeIndexMetaTable]:
        try:
            return self._output_table
        except AttributeError as exc:
            raise AttributeError(
                "TimeIndexTableUpdater output_table has not been initialized. "
                "Pass output_table=... to TimeIndexTableUpdater.__init__."
            ) from exc

    @output_table.setter
    def output_table(self, value: type[PlatformTimeIndexMetaTable]) -> None:
        self._output_table = ensure_registered_output_table(
            value,
            context="TimeIndexTableUpdater",
        )

    @property
    def output_metadata(self) -> Any:
        output_metadata = self.output_table.get_time_index_meta_table()
        if output_metadata is None:
            raise ValueError(
                "TimeIndexTableUpdater output_table registration metadata is unavailable after register()."
            )
        return output_metadata

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        """Creates config from init args and sets them as instance attributes."""
        logger.debug(f"Creating configuration for {self.__class__.__name__}")

        init_kwargs["table_updater_class_import_path"] = {
            "module": self.__class__.__module__,
            "qualname": self.__class__.__qualname__,
        }

        config = configuration.create_config(
            kwargs=init_kwargs,
            updater_class_name=self.__class__.__name__,
        )

        for field_name, value in asdict(config).items():
            if field_name == "storage_hash":
                continue
            setattr(self, field_name, value)

    def _get_table_update_configuration(self) -> BaseConfiguration | None:
        config = getattr(self, "config", None)
        return config if isinstance(config, BaseConfiguration) else None

    @property
    def hash_namespace(self) -> str:
        """
        Return the active hash namespace for this updater.

        An empty string means "no namespace", which keeps hashing identical to the
        normal production-style behavior. A non-empty value means this updater was
        constructed in an isolated namespace and its hashes include that namespace.
        """
        return getattr(self, "_hash_namespace", "") or ""

    def get_offset_start(self) -> datetime.datetime:
        """
        Return the configured first-run fallback start date.
        """
        config = self._get_table_update_configuration()
        offset_start = getattr(config, "offset_start", None) if config is not None else None
        if offset_start is not None:
            return config.offset_start
        return self.OFFSET_START

    @property
    def data_source_uid(self) -> str:
        data_source_uid = self.output_table.get_data_source_uid()
        if data_source_uid in (None, ""):
            raise ValueError(
                "TimeIndexTableUpdater data_source_uid requires output_table data-source identity."
            )
        return str(data_source_uid)

    @property
    def output_table_uid(self) -> str:
        uid = getattr(self.output_metadata, "uid", None)
        if uid in (None, ""):
            raise ValueError(
                "TimeIndexTableUpdater output_table requires canonical TimeIndexMetaTable uid."
            )
        return str(uid)

    @property
    def table_update(self) -> TimeIndexTableUpdate:
        """The persisted backend update-process resource."""
        return self.update_manager.table_update

    @property
    def update_manager(self) -> TimeIndexTableUpdateManager:
        if self._update_manager is None:
            self.logger.debug(f"Setting table update manager for {self.update_hash}")
            self._set_update_manager(update_hash=self.update_hash)
        return self._update_manager

    @property
    def table_accessor(self) -> TimeIndexTableUpdateManager:
        return self.update_manager

    @property
    def data_source(self) -> Any:
        data_source = getattr(self.output_metadata, "data_source", None)
        if data_source is not None:
            return data_source
        return self.update_manager.data_source

    # --- Persistence & Backend Methods ---

    @tracer.start_as_current_span("TableUpdate: set_state_with_sessions")
    def _set_state_with_sessions(
        self,
        include_client_objects: bool = True,
        graph_depth_limit: int = 1000,
        graph_depth: int = 0,
    ) -> None:
        """
        Attaches runtime sessions and synchronizes backend metadata after rebuild.

        Args:
            include_client_objects: Whether to include nested client objects.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.
        """
        if graph_depth_limit == -1:
            graph_depth_limit = 1e6

        minimum_required_depth_for_update = self.get_minimum_required_depth_for_update()

        if graph_depth_limit < minimum_required_depth_for_update and graph_depth == 0:
            graph_depth_limit = minimum_required_depth_for_update
            self.logger.warning(
                f"Graph depth limit overwritten to {minimum_required_depth_for_update}"
            )

        output_data_source = getattr(self.output_metadata, "data_source", None)
        if isinstance(output_data_source, dict):
            relation_type = output_data_source.get("class_type")
        else:
            relation_type = getattr(output_data_source, "class_type", None)

        if graph_depth <= graph_depth_limit and relation_type:
            self._set_update_manager(
                update_hash=self.update_hash,
                table_update=None,
            )

        self.update_manager.synchronize_table_update(table_update=None)

    def _set_update_manager(
        self,
        update_hash: str,
        table_update: Any | None = None,
    ) -> None:
        """
        Initialize the manager that synchronizes this updater with backend state.

        Args:
           update_hash : str
               Canonical update-process hash.
           table_update : Any | None, optional
               An already-resolved backend update resource, if available.
        """
        self._update_manager = TimeIndexTableUpdateManager.get_from_output_table(
            output_table=self.output_table,
            update_hash=update_hash,
            class_name=self.__class__.__name__,
            table_update=table_update,
        )

    def verify_and_build_remote_objects(self) -> None:
        """
        Verifies and builds remote objects by calling the persistence layer.
        This logic is now correctly located within the BuildManager.
        """
        self.update_manager.ensure_table_update(
            local_configuration=self.local_initial_configuration,
        )

    def set_relation_tree(
        self,
        *,
        force_rebuild: bool = False,
        _visited_update_uids: set[str] | None = None,
    ):
        """Persist declared updater and table dependency relationships."""

        if self.update_manager.table_update is None:
            self.verify_and_build_remote_objects()  #
        table_update_uid = str(getattr(self.update_manager.table_update, "uid", ""))
        if _visited_update_uids is None:
            _visited_update_uids = set()
        if table_update_uid:
            if table_update_uid in _visited_update_uids:
                return
            _visited_update_uids.add(table_update_uid)

        if force_rebuild:
            self.update_manager.clear_dependencies()
            self.depth_df = pd.DataFrame()
            self.dependencies_df = None
        elif self.update_manager.is_local_relation_tree_set():
            return
        declared_dependencies = self.dependencies() or {}

        for name, dependency in declared_dependencies.items():
            self.logger.debug(f"Connecting dependency '{name}'...")

            if isinstance(dependency, TimeIndexTableUpdater):
                dependency.verify_and_build_remote_objects()
                self.update_manager.connect_update_dependency(dependency)
                dependency.set_relation_tree(
                    force_rebuild=force_rebuild,
                    _visited_update_uids=_visited_update_uids,
                )
            elif isinstance(dependency, TimeIndexTableRef):
                self.update_manager.connect_table_dependency(dependency)
            else:
                raise TypeError(
                    f"Dependency {name!r} must be TimeIndexTableUpdater or "
                    f"TimeIndexTableRef; got {type(dependency).__name__}."
                )

        self.update_manager.set_ogm_dependencies_linked()

    def set_dependencies_df(self):
        depth_df = self.update_manager.get_all_dependencies_update_priority()
        self.depth_df = depth_df
        if not depth_df.empty:
            if "update_node_uid" not in depth_df.columns:
                raise ValueError("Dependency dataframe must include 'update_node_uid'.")
            if self.table_update.uid is None:
                raise ValueError(
                    "TimeIndexTableUpdate must have uid before filtering dependencies."
                )
            self.dependencies_df = depth_df[
                depth_df["update_node_uid"].astype(str) != str(self.table_update.uid)
            ].copy()

        else:
            self.dependencies_df = pd.DataFrame()

    def get_update_statistics(self):
        """
        This method always queries last state
        """
        return self.update_manager.output_metadata.get_data_updates()

    def prepare_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Hook for subclasses to scope or enrich update statistics before update()."""
        return update_statistics

    def _set_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Attach generic update statistics."""
        update_statistics = self.prepare_update_statistics(update_statistics)
        self.update_statistics = update_statistics
        return update_statistics

    def run(
        self,
        debug_mode: bool = True,
        *,
        update_tree: bool = True,
        force_update: bool = False,
        update_only_tree: bool = False,
        remote_scheduler: object | None = None,
        override_update_stats: BaseUpdateStatistics | None = None,
    ):
        """
        Run one update cycle for this updater.

        By default, this also updates dependencies first, validates output, persists rows,
        and runs metadata/post-update hooks.

        Namespace behavior:
        if this updater has a non-empty ``hash_namespace``, ``run()`` activates that
        namespace around the full run. That ensures dependencies instantiated inside
        ``dependencies()`` inherit the same namespace instead of accidentally writing
        into the non-namespaced tables.

        Parameters
        ----------
        debug_mode : bool, default=True
            Enables debug-friendly run behavior.
        update_tree : bool, default=True
            If ``True``, update dependencies before this updater.
        force_update : bool, default=False
            If ``True``, run even when no new range is detected.
        update_only_tree : bool, default=False
            If ``True``, update dependencies only (skip this updater).
        remote_scheduler : object | None, optional
            Optional scheduler context.
        override_update_stats : BaseUpdateStatistics | None, optional
            Optional explicit update-state object (useful in tests or controlled runs).

        Returns
        -------
        Any
            Result returned by ``UpdateRunner.run()``.
        """

        require_code_repository_branch_context("TimeIndexTableUpdater.run")
        # Execution remains sequential; distributed execution is not part of this contract.
        debug_mode = True

        def _do_run():
            update_runner = runner.UpdateRunner(
                table_updater=self,
                debug_mode=debug_mode,
                force_update=force_update,
                update_tree=update_tree,
                update_only_tree=update_only_tree,
                remote_scheduler=remote_scheduler,
                override_update_stats=override_update_stats,
            )
            return update_runner.run()

        # IMPORTANT:
        # If this updater is namespaced, make that namespace active for the full run.
        # That ensures dependencies() calls also create namespaced updaters automatically.
        if self.hash_namespace:
            with _hash_namespace_cm(self.hash_namespace):
                return _do_run()

        return _do_run()

    # --- Optional Hooks for Customization ---
    def run_after_post_init_routines(self) -> None:
        pass

    def get_minimum_required_depth_for_update(self) -> int:
        """
        Controls the minimum depth that needs to be rebuilt.
        """
        return 0

    def run_post_update_routines(
        self,
        error_on_last_update: bool,
    ) -> None:
        """Should be overwritten by subclass"""
        pass

    @staticmethod
    def _max_time_index_from_update_statistics(
        table_update_run: Any,
    ) -> datetime.datetime | None:
        update_statistics = getattr(table_update_run, "update_statistics", None)
        max_time_index_value = getattr(update_statistics, "max_time_index_value", None)
        if max_time_index_value is not None:
            return max_time_index_value
        return None

    def _validate_update_output(self, temp_df: pd.DataFrame) -> None:
        output_data_source = getattr(self.output_metadata, "data_source", None)
        if output_data_source is None:
            output_data_source = self.update_manager.data_source
        if isinstance(output_data_source, dict):
            class_type = output_data_source.get("class_type")
        else:
            class_type = getattr(output_data_source, "class_type", None)

        runner.UpdateRunner.validate_data_frame(
            temp_df,
            class_type,
            meta_table=self.output_metadata,
        )

    def _execute_local_update(
        self,
        table_update_run: Any,
    ) -> LocalUpdateResult:
        self.logger.debug(f"Calculating update for {self}...")

        temp_df = self.update()
        update_statistics_max_time_index = self._max_time_index_from_update_statistics(
            table_update_run
        )

        if temp_df is None:
            raise Exception(f" {self} update(...) method needs to return a data frame")

        if temp_df.empty:
            self.logger.warning(f"{self} produced no new data in this update round.")
            return temp_df

        if update_statistics_max_time_index is not None and not SessionDataSource.is_local_db:
            temp_df = self.update_statistics.filter_df_by_latest_value(temp_df)

        if temp_df.empty:
            self.logger.warning(f"No new data to persist for {self} after filtering.")
            return temp_df

        self._validate_update_output(temp_df)

        self.logger.info(f"Persisting {len(temp_df)} new rows for {self}.")
        self.update_manager.persist_updated_data(
            temp_df=temp_df,
            overwrite=(update_statistics_max_time_index is not None),
        )
        self.logger.info(f"Successfully updated {self}.")
        return temp_df

    @abstractmethod
    def dependencies(self) -> dict[str, TableDependency]:
        """
        Return direct upstream dependencies required by this updater.

        Keep keys short and descriptive (for example ``"prices"`` or ``"raw"``),
        and prefer creating dependency instances in ``__init__``.

        Returns:
            A dictionary whose values are executable updaters or read-only table
            references.
        """
        raise NotImplementedError

    @abstractmethod
    def update(self) -> LocalUpdateResult:
        """
        Build and return the update payload for this run.

        Default ``TimeIndexTableUpdater`` execution expects a ``pd.DataFrame`` and will use
        ``self.update_statistics`` to compute an incremental window before
        persistence.

        Specialized subclasses that override ``_execute_local_update(...)``
        may return a different ``LocalUpdateResult`` shape instead.

        For the default DataFrame-based path, the expected output shape is:

        - index starts with ``time_index`` (UTC-aware datetimes),
        - column names are lowercase and schema-stable,
        - no duplicate index keys,
        - temporal payload columns are allowed when declared with an explicit
          dtype such as ``date`` or ``datetime64[ns, UTC]``.

        MultiIndex tables should use the configured time-first index vector.
        Any identity dimensions after ``time_index`` must match the table's
        source-table configuration.

        Returns
        -------
        LocalUpdateResult
            The payload produced for this run. For the default DataFrame-based
            path, return an empty ``pd.DataFrame`` when there is nothing new to
            persist. Returning ``None`` is invalid and will raise at runtime.
        """
        raise NotImplementedError


configuration.serialize_argument.register(
    TimeIndexTableUpdater, configuration._serialize_table_updater
)
