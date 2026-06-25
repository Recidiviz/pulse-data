# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""The registry of LLM models that document extractors may call, parsed from the
single global `model_registry.yaml`. The registry declares model families (API
access info), base models (fixed traits and tunable-parameter catalogs), and
named model configs (reusable parameter bundles) that extractors reference by
name instead of repeating model settings.
"""
import abc
from enum import Enum
from pathlib import Path
from types import ModuleType
from typing import Generic, TypeVar

import attr

from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.documents import config as default_config_module
from recidiviz.utils.yaml_dict import YAMLDict

MODEL_REGISTRY_FILENAME = "model_registry.yaml"

THINKING_BUDGET_TOKENS_PARAMETER_NAME = "thinking_budget_tokens"
"""Name of the tunable parameter that caps a thinking model's internal
reasoning tokens. A value of 0 disables thinking; omitting it uses
model-managed dynamic thinking.
"""


class LLMAPIProvider(Enum):
    """API providers we can route LLM requests through."""

    VERTEX_AI = "vertex_ai"


class LLMModelParameterType(Enum):
    """The value type of a tunable model parameter, as declared by the `type`
    key in a base model's `parameters` block.
    """

    FLOAT = "FLOAT"
    INTEGER = "INTEGER"


NumericalParameterT = TypeVar("NumericalParameterT", int, float)


@attr.define(frozen=True, kw_only=True)
class LLMModelNumericalParameterDefinition(Generic[NumericalParameterT], abc.ABC):
    """Catalog entry for a single tunable parameter of a base model, declaring
    the range of values a model config may set it to. Subclasses fix the
    parameter's value type.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the parameter as passed to the model API (e.g. `temperature`)."""

    @property
    @abc.abstractmethod
    def min_value_inclusive(self) -> NumericalParameterT:
        """Smallest value (inclusive) a model config may set this parameter to."""

    @property
    @abc.abstractmethod
    def max_value_inclusive(self) -> NumericalParameterT:
        """Largest value (inclusive) a model config may set this parameter to."""

    description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """Human-readable description of what this parameter controls."""

    is_optional: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether a model config may omit a value for this parameter. When false,
    every config on this base model must set a value.
    """

    def __attrs_post_init__(self) -> None:
        if self.min_value_inclusive > self.max_value_inclusive:
            raise ValueError(
                f"Parameter [{self.name}] declares an invalid range: minimum "
                f"[{self.min_value_inclusive}] is greater than maximum "
                f"[{self.max_value_inclusive}]."
            )

    def validate_value(self, value: int | float) -> None:
        """Validates that |value| falls within the declared allowed range."""
        if not self.min_value_inclusive <= value <= self.max_value_inclusive:
            raise ValueError(
                f"Value [{value}] for parameter [{self.name}] is outside "
                f"the allowed range [{self.min_value_inclusive}, "
                f"{self.max_value_inclusive}]."
            )

    @staticmethod
    def from_yaml_dict(
        *, name: str, yaml_dict: YAMLDict
    ) -> "LLMModelNumericalParameterDefinition":
        """Returns the parameter definition parsed from one entry of a base
        model's `parameters` block, choosing the definition subclass that
        matches the declared `type`.
        """
        range_values = yaml_dict.pop("allowed_range", list)
        if len(range_values) != 2:
            raise ValueError(
                f"Parameter [{name}] must declare its allowed_range as a "
                f"two-element [min, max] list, found [{range_values}]."
            )

        parameter_type = LLMModelParameterType(yaml_dict.pop("type", str))

        definition_cls: type[LLMModelNumericalParameterDefinition]
        if parameter_type is LLMModelParameterType.INTEGER:
            definition_cls = LLMModelIntegerParameterDefinition
        elif parameter_type is LLMModelParameterType.FLOAT:
            definition_cls = LLMModelFloatParameterDefinition
        else:
            raise ValueError(f"Unexpected parameter_type: [{parameter_type}]")

        definition = definition_cls(
            name=name,
            min_value_inclusive=range_values[0],
            max_value_inclusive=range_values[1],
            description=yaml_dict.pop("description", str),
            is_optional=yaml_dict.pop_optional("is_optional", bool) or False,
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for parameter [{name}]: "
                f"{repr(yaml_dict.get())}"
            )
        return definition


@attr.define(frozen=True, kw_only=True)
class LLMModelIntegerParameterDefinition(LLMModelNumericalParameterDefinition[int]):
    """Catalog entry for an integer-valued tunable parameter."""

    _min_value_inclusive: int = attr.ib(
        alias="min_value_inclusive", validator=attr_validators.is_int_strict
    )
    """Smallest value (inclusive) a model config may set this parameter to."""

    _max_value_inclusive: int = attr.ib(
        alias="max_value_inclusive", validator=attr_validators.is_int_strict
    )
    """Largest value (inclusive) a model config may set this parameter to."""

    @property
    def min_value_inclusive(self) -> int:
        return self._min_value_inclusive

    @property
    def max_value_inclusive(self) -> int:
        return self._max_value_inclusive


@attr.define(frozen=True, kw_only=True)
class LLMModelFloatParameterDefinition(LLMModelNumericalParameterDefinition[float]):
    """Catalog entry for a float-valued tunable parameter. Integer values are
    accepted wherever a float is expected (e.g. `temperature: 0`) and stored
    unchanged.
    """

    _min_value_inclusive: float = attr.ib(
        alias="min_value_inclusive", validator=attr_validators.is_numerical_strict
    )
    """Smallest value (inclusive) a model config may set this parameter to."""

    _max_value_inclusive: float = attr.ib(
        alias="max_value_inclusive", validator=attr_validators.is_numerical_strict
    )
    """Largest value (inclusive) a model config may set this parameter to."""

    @property
    def min_value_inclusive(self) -> float:
        return self._min_value_inclusive

    @property
    def max_value_inclusive(self) -> float:
        return self._max_value_inclusive


@attr.define(frozen=True, kw_only=True)
class LLMModelIntegerParameterValue:
    """A value chosen for an integer-valued tunable parameter in a model config,
    paired with the parameter's catalog definition. Validates on construction
    that the value falls within the definition's allowed range.
    """

    parameter_definition: LLMModelIntegerParameterDefinition = attr.ib(
        validator=attr.validators.instance_of(LLMModelIntegerParameterDefinition)
    )
    """The parameter this value is for."""

    value: int = attr.ib(validator=attr_validators.is_int_strict)
    """The integer value of this parameter."""

    def __attrs_post_init__(self) -> None:
        self.parameter_definition.validate_value(self.value)


@attr.define(frozen=True, kw_only=True)
class LLMModelFloatParameterValue:
    """A value chosen for a float-valued tunable parameter in a model config,
    paired with the parameter's catalog definition. Validates on construction
    that the value falls within the definition's allowed range.
    """

    parameter_definition: LLMModelFloatParameterDefinition = attr.ib(
        validator=attr.validators.instance_of(LLMModelFloatParameterDefinition)
    )
    """The parameter this value is for."""

    value: float | int = attr.ib(validator=attr_validators.is_numerical_strict)
    """The numerical value of this parameter."""

    def __attrs_post_init__(self) -> None:
        self.parameter_definition.validate_value(self.value)


@attr.define(frozen=True, kw_only=True)
class LLMModelFamily:
    """Provider API-access info shared by a family of models (e.g. all Gemini
    models).
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Key that base models use to reference this family (e.g. `gemini`)."""

    default_api_provider: LLMAPIProvider = attr.ib(
        validator=attr.validators.in_(LLMAPIProvider)
    )
    """The API provider requests to this family's models are routed through."""

    @classmethod
    def from_yaml_dict(cls, *, name: str, yaml_dict: YAMLDict) -> "LLMModelFamily":
        """Returns the family parsed from one entry of the `model_families`
        block.
        """
        family = cls(
            name=name,
            default_api_provider=LLMAPIProvider(
                yaml_dict.pop("default_api_provider", str)
            ),
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for model family [{name}]: "
                f"{repr(yaml_dict.get())}"
            )
        return family


@attr.define(frozen=True, kw_only=True)
class BaseLLMModel:
    """A model's fixed traits: its approved pinned versions, tunable-parameter
    catalog, context window size, and capability flags.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Key that identifies this base model in the registry (e.g.
    `gemini-2.5-flash`).
    """

    family: LLMModelFamily = attr.ib(
        validator=attr.validators.instance_of(LLMModelFamily)
    )
    """The model family this model belongs to."""

    is_thinking_model: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether this model generates thinking tokens by default."""

    input_token_limit: int = attr.ib(validator=attr_validators.is_positive_int)
    """The model's context window, in tokens."""

    supports_structured_output: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the model supports schema-constrained (strict JSON schema)
    output.
    """

    supports_implicit_caching_in_batch: bool = attr.ib(
        validator=attr_validators.is_bool
    )
    """Whether implicit prompt caching is available in batch prediction. Models
    without this cannot benefit from cached-token discounts in batch jobs.
    """

    known_pinned_versions: list[str] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(str),
        ]
    )
    """Allowlist of approved pinned version strings for this model."""

    parameters: dict[str, LLMModelNumericalParameterDefinition] = attr.ib(
        validator=attr_validators.is_dict_of(str, LLMModelNumericalParameterDefinition)
    )
    """Catalog of tunable parameters configs may set, keyed by parameter name."""

    def __attrs_post_init__(self) -> None:
        duplicate_versions = {
            v
            for v in self.known_pinned_versions
            if self.known_pinned_versions.count(v) > 1
        }
        if duplicate_versions:
            raise ValueError(
                f"Base model [{self.name}] declares duplicate pinned versions: "
                f"{sorted(duplicate_versions)}."
            )
        for parameter_name, definition in self.parameters.items():
            if parameter_name != definition.name:
                raise ValueError(
                    f"Base model [{self.name}] has a parameter registered under "
                    f"key [{parameter_name}] whose definition is named "
                    f"[{definition.name}]."
                )

    def parameter_definition(
        self, parameter_name: str
    ) -> LLMModelNumericalParameterDefinition:
        """Returns the catalog entry for |parameter_name|, raising if this model
        does not declare such a parameter.
        """
        if parameter_name not in self.parameters:
            raise ValueError(
                f"Parameter [{parameter_name}] is not a tunable parameter of "
                f"base model [{self.name}]. Allowed parameters: "
                f"{sorted(self.parameters)}."
            )
        return self.parameters[parameter_name]

    @classmethod
    def from_yaml_dict(
        cls,
        *,
        name: str,
        yaml_dict: YAMLDict,
        model_families_by_name: dict[str, LLMModelFamily],
    ) -> "BaseLLMModel":
        """Returns the base model parsed from one entry of the `base_models`
        block, resolving its family against |model_families_by_name|.
        """
        family_name = yaml_dict.pop("family", str)
        if family_name not in model_families_by_name:
            raise ValueError(
                f"Base model [{name}] references unknown model family "
                f"[{family_name}]. Known families: {sorted(model_families_by_name)}."
            )

        parameters = {}
        if (parameters_dict := yaml_dict.pop_dict_optional("parameters")) is not None:
            for parameter_name in parameters_dict.keys():
                parameters[
                    parameter_name
                ] = LLMModelNumericalParameterDefinition.from_yaml_dict(
                    name=parameter_name,
                    yaml_dict=parameters_dict.pop_dict(parameter_name),
                )

        base_model = cls(
            name=name,
            family=model_families_by_name[family_name],
            is_thinking_model=yaml_dict.pop("is_thinking_model", bool),
            input_token_limit=yaml_dict.pop("input_token_limit", int),
            supports_structured_output=yaml_dict.pop(
                "supports_structured_output", bool
            ),
            supports_implicit_caching_in_batch=yaml_dict.pop(
                "supports_implicit_caching_in_batch", bool
            ),
            known_pinned_versions=yaml_dict.pop_list("known_pinned_versions", str),
            parameters=parameters,
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for base model [{name}]: "
                f"{repr(yaml_dict.get())}"
            )
        return base_model


@attr.define(frozen=True, kw_only=True)
class LLMModelConfig:
    """A named, reusable bundle of model parameters that extractors reference by
    name (e.g. `GEMINI_2_5_FLASH_NO_THINKING`) instead of repeating model
    settings. Fully resolved: holds the base model whose traits (context window,
    capability flags) downstream consumers read directly off this config.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name extractors use to reference this config."""

    base_model: BaseLLMModel = attr.ib(
        validator=attr.validators.instance_of(BaseLLMModel)
    )
    """The base model that declares |model|, including the valid |parameter_values|."""

    model: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The pinned model version string requests are made against. Must be one of
    the base model's `known_pinned_versions`.
    """

    parameter_values: dict[
        str, LLMModelIntegerParameterValue | LLMModelFloatParameterValue
    ] = attr.ib(
        validator=attr_validators.is_dict_of(
            str, (LLMModelIntegerParameterValue, LLMModelFloatParameterValue)
        )
    )
    """Parameter values to set on each request, keyed by parameter name. Every
    parameter must be declared in the base model's catalog and fall within its
    declared range.
    """

    def __attrs_post_init__(self) -> None:
        if self.model not in self.base_model.known_pinned_versions:
            raise ValueError(
                f"Model config [{self.name}] uses model [{self.model}], which is "
                f"not in base model [{self.base_model.name}]'s "
                f"known_pinned_versions: {self.base_model.known_pinned_versions}."
            )
        missing_parameter_names = sorted(
            parameter_name
            for parameter_name, definition in self.base_model.parameters.items()
            if not definition.is_optional
            and parameter_name not in self.parameter_values
        )
        if missing_parameter_names:
            raise ValueError(
                f"Model config [{self.name}] is missing values for required "
                f"parameters of base model [{self.base_model.name}]: "
                f"{missing_parameter_names}."
            )

    @property
    def api_provider(self) -> LLMAPIProvider:
        return self.base_model.family.default_api_provider

    @property
    def is_thinking_model(self) -> bool:
        return self.base_model.is_thinking_model

    @property
    def enables_thinking(self) -> bool:
        """Whether requests made with this config generate thinking tokens.
        False for a non-thinking base model, or for a thinking-capable model
        whose `thinking_budget_tokens` is explicitly set to 0. A thinking-capable
        model that omits the budget uses model-managed dynamic thinking, so
        thinking is enabled.
        """
        if not self.base_model.is_thinking_model:
            return False
        if THINKING_BUDGET_TOKENS_PARAMETER_NAME not in self.parameter_values:
            return True
        return self.parameter_values[THINKING_BUDGET_TOKENS_PARAMETER_NAME].value != 0

    @property
    def input_token_limit(self) -> int:
        return self.base_model.input_token_limit

    @property
    def supports_structured_output(self) -> bool:
        return self.base_model.supports_structured_output

    @property
    def supports_implicit_caching_in_batch(self) -> bool:
        return self.base_model.supports_implicit_caching_in_batch

    @classmethod
    def from_yaml_dict(
        cls,
        *,
        name: str,
        yaml_dict: YAMLDict,
        base_model_by_pinned_version: dict[str, BaseLLMModel],
    ) -> "LLMModelConfig":
        """Returns the model config parsed from one entry of the `configs`
        block, resolving its base model via the pinned version it declares. All
        keys other than `model` are parameter values validated against the base
        model's parameter catalog.
        """
        model_pinned_version = yaml_dict.pop("model", str)
        if model_pinned_version not in base_model_by_pinned_version:
            raise ValueError(
                f"Model config [{name}] uses model [{model_pinned_version}], which is "
                f"not in any base model's known_pinned_versions. Known versions: "
                f"{sorted(base_model_by_pinned_version)}."
            )
        base_model = base_model_by_pinned_version[model_pinned_version]

        parameter_values = {}
        for parameter_name in yaml_dict.keys():
            definition = base_model.parameter_definition(parameter_name)
            value = yaml_dict.pop(parameter_name, object)
            parameter_value: LLMModelIntegerParameterValue | LLMModelFloatParameterValue
            if isinstance(definition, LLMModelIntegerParameterDefinition):
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ValueError(
                        f"Parameter [{parameter_name}] in model config [{name}] "
                        f"must be an integer, found [{value!r}]."
                    )
                parameter_value = LLMModelIntegerParameterValue(
                    parameter_definition=definition, value=value
                )
            elif isinstance(definition, LLMModelFloatParameterDefinition):
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(
                        f"Parameter [{parameter_name}] in model config [{name}] "
                        f"must be numeric, found [{value!r}]."
                    )
                parameter_value = LLMModelFloatParameterValue(
                    parameter_definition=definition, value=value
                )
            else:
                raise ValueError(
                    f"Unexpected parameter definition type [{type(definition)}] "
                    f"for parameter [{parameter_name}]."
                )

            parameter_values[parameter_name] = parameter_value

        return cls(
            name=name,
            base_model=base_model,
            model=model_pinned_version,
            parameter_values=parameter_values,
        )


def _get_base_models_by_pinned_version(
    base_models: list[BaseLLMModel],
) -> dict[str, BaseLLMModel]:
    """Returns a map of pinned version string to the base model that declares
    it, raising if any pinned version is declared by more than one base model.
    """
    index: dict[str, BaseLLMModel] = {}
    for base_model in base_models:
        for pinned_version in base_model.known_pinned_versions:
            if pinned_version in index:
                raise ValueError(
                    f"Pinned version [{pinned_version}] is declared by multiple "
                    f"base models: [{index[pinned_version].name}] and "
                    f"[{base_model.name}]."
                )
            index[pinned_version] = base_model
    return index


@attr.define(frozen=True, kw_only=True)
class LLMModelRegistry:
    """The parsed `model_registry.yaml`: every model family, base model, and
    named model config that document extractors may use. Resolves a model config
    name to a fully-resolved LLMModelConfig.
    """

    model_families: list[LLMModelFamily] = attr.ib(
        validator=attr_validators.is_list_of(LLMModelFamily)
    )
    """All model families."""

    model_families_by_name: dict[str, LLMModelFamily] = attr.ib(
        init=False, validator=attr_validators.is_dict_of(str, LLMModelFamily)
    )
    """All model families, keyed by family name."""

    @model_families_by_name.default
    def _build_model_families_by_name(self) -> dict[str, LLMModelFamily]:
        model_families_by_name: dict[str, LLMModelFamily] = {}
        for family in self.model_families:
            if family.name in model_families_by_name:
                raise ValueError(
                    f"Found multiple model families with name [{family.name}]."
                )
            model_families_by_name[family.name] = family
        return model_families_by_name

    base_models: list[BaseLLMModel] = attr.ib(
        validator=attr_validators.is_list_of(BaseLLMModel)
    )
    """All base models."""

    base_models_by_name: dict[str, BaseLLMModel] = attr.ib(
        init=False, validator=attr_validators.is_dict_of(str, BaseLLMModel)
    )
    """All base models, keyed by base model name."""

    @base_models_by_name.default
    def _build_base_models_by_name(self) -> dict[str, BaseLLMModel]:
        base_models_by_name: dict[str, BaseLLMModel] = {}
        for base_model in self.base_models:
            if base_model.name in base_models_by_name:
                raise ValueError(
                    f"Found multiple base models with name [{base_model.name}]."
                )
            base_models_by_name[base_model.name] = base_model
        return base_models_by_name

    model_configs: list[LLMModelConfig] = attr.ib(
        validator=attr_validators.is_list_of(LLMModelConfig)
    )
    """All named model configs."""

    model_configs_by_name: dict[str, LLMModelConfig] = attr.ib(
        init=False, validator=attr_validators.is_dict_of(str, LLMModelConfig)
    )
    """All named model configs, keyed by config name."""

    @model_configs_by_name.default
    def _build_model_configs_by_name(self) -> dict[str, LLMModelConfig]:
        model_configs_by_name: dict[str, LLMModelConfig] = {}
        for config in self.model_configs:
            if config.name in model_configs_by_name:
                raise ValueError(
                    f"Found multiple model configs with name [{config.name}]."
                )
            model_configs_by_name[config.name] = config
        return model_configs_by_name

    def __attrs_post_init__(self) -> None:
        # Enforce that pinned versions are not repeated in multiple base models
        _get_base_models_by_pinned_version(self.base_models)

    def get_model_config(self, model_config_name: str) -> LLMModelConfig:
        """Returns the fully-resolved model config registered under
        |model_config_name|, raising if no such config exists.
        """
        if model_config_name not in self.model_configs_by_name:
            raise ValueError(
                f"No model config named [{model_config_name}] in the model "
                f"registry. Known configs: {sorted(self.model_configs_by_name)}."
            )
        return self.model_configs_by_name[model_config_name]

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "LLMModelRegistry":
        """Returns the registry parsed from the `model_registry.yaml` at
        |yaml_path|.
        """
        registry_dict = YAMLDict.from_path(yaml_path)

        families_dict = registry_dict.pop_dict("model_families")
        model_families = [
            LLMModelFamily.from_yaml_dict(
                name=family_name, yaml_dict=families_dict.pop_dict(family_name)
            )
            for family_name in families_dict.keys()
        ]
        model_families_by_name = {family.name: family for family in model_families}

        base_models_dict = registry_dict.pop_dict("base_models")
        base_models = [
            BaseLLMModel.from_yaml_dict(
                name=base_model_name,
                yaml_dict=base_models_dict.pop_dict(base_model_name),
                model_families_by_name=model_families_by_name,
            )
            for base_model_name in base_models_dict.keys()
        ]
        base_model_by_pinned_version = _get_base_models_by_pinned_version(base_models)

        configs_dict = registry_dict.pop_dict("configs")
        model_configs = [
            LLMModelConfig.from_yaml_dict(
                name=config_name,
                yaml_dict=configs_dict.pop_dict(config_name),
                base_model_by_pinned_version=base_model_by_pinned_version,
            )
            for config_name in configs_dict.keys()
        ]

        if registry_dict:
            raise ValueError(
                f"Found unexpected top-level keys in model registry at "
                f"[{yaml_path}]: {repr(registry_dict.get())}"
            )

        return cls(
            model_families=model_families,
            base_models=base_models,
            model_configs=model_configs,
        )


def model_registry_yaml_path(config_module: ModuleType | None = None) -> Path:
    """Returns the path to the model registry YAML file within |config_module|
    (the production config package by default).
    """
    module = config_module or default_config_module
    if module.__file__ is None:
        raise ValueError(f"No file associated with module [{module}].")
    return Path(module.__file__).parent / MODEL_REGISTRY_FILENAME


def load_llm_model_registry(
    config_module: ModuleType | None = None,
) -> LLMModelRegistry:
    """Returns the model registry parsed from the registry YAML within
    |config_module| (the production config package by default).
    """
    return LLMModelRegistry.from_yaml(model_registry_yaml_path(config_module))
