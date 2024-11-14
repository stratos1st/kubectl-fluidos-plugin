'''
------------------------------------------------------------------------------
Copyright 2023 IBM Research Europe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
------------------------------------------------------------------------------
'''
from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable
from typing import Any
from typing import TextIO

import yaml

from .modelbased import ModelBasedOrchestratorConfiguration
from .modelbased import ModelBasedOrchestratorProcessor
from .mspl import MSPLProcessor
from .mspl import MSPLProcessorConfiguration

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader  # type: ignore

from xml.etree import ElementTree
from enum import Enum
from enum import auto


logger = logging.getLogger(__name__)


class InputFormat(Enum):
    K8S = auto()
    MSPL = auto()
    KFP = auto() 


INTENT_K8S_KEYWORD = "fluidos-intent-"  # label to be confirmed


def _is_YAML(data: str) -> bool:
    try:
        _ = _to_YAML(data)
        return True
    except Exception as e:
        logger.info(str(e))
    return False


def _is_XML(data: str) -> bool:
    try:
        _ = ElementTree.fromstring(data)
        return True
    except Exception as e:
        logger.info(str(e))
    return False


def _to_YAML(data: str) -> dict[str, Any]:
    return yaml.load(data, Loader=Loader)


def _check_input_format(input_data: str) -> tuple[InputFormat, dict[str, Any]]:
    if _is_XML(input_data):
        return (InputFormat.MSPL, dict())
    elif _is_YAML(input_data):
        yaml_data = _to_YAML(input_data)
        if _is_kubeflow_pipeline(yaml_data):  # New check for Kubeflow pipeline
            return (InputFormat.KFP, yaml_data)
        return (InputFormat.K8S, yaml_data)
    raise ValueError("Unknown format")

def _has_intent_defined(spec: dict[str, Any]) -> bool:
    annotations: dict[str, str] = spec.get("metadata", dict()).get("annotations", dict())
    key: str

    for key in annotations.keys():
        if key.startswith(INTENT_K8S_KEYWORD):
            return True

    return False


def _read_file_argument_content(filename: str) -> str:
    with open(filename) as input_file:
        return input_file.read()


def _attempt_reading_from_stdio(stdin: TextIO) -> str:
    if stdin.isatty():
        return ''
    else:
        return stdin.read()


def _extract_input_data(arguments: list[str], stdin: TextIO) -> tuple[list[str], str | None]:
    input_data: list[str] = [
        _read_file_argument_content(arguments[idx + 1]) for idx, arg in enumerate(arguments) if (arg == "-f" or arg == "--filename") and idx + 1 < len(arguments)
    ]

    if len(input_data):
        return (input_data, None)

    stdin_data = _attempt_reading_from_stdio(stdin)

    if stdin_data and len(stdin_data):
        return ([], None)

    raise ValueError("No input provided")


def _is_deployment(spec: dict[str, Any]) -> bool:
    if type(spec) is dict:
        return spec.get("kind", None) == "Deployment"
    return False

def _is_kubeflow_pipeline(spec: dict[str, Any]) -> bool:
    """Detects if the YAML spec is a Kubeflow Pipeline resource."""
    return spec.get("apiVersion", "").startswith("kubeflow.org") and spec.get("kind") in {"Pipeline", "PipelineRun"}


def _behavior_not_defined() -> int:
    raise NotImplementedError()


def _default_apply(args: list[str], stdin: str | None) -> int:
    return os.system("kubectl apply " + " ".join(args))




def fluidos_kubectl_extension(
    argv: list[str], 
    stdin: TextIO, 
    *, 
    on_apply: Callable[[list[str], str | None], int] = _default_apply, 
    on_mlps: Callable[..., int] = _behavior_not_defined, 
    on_k8s_w_intent: Callable[..., int] = _behavior_not_defined, 
    on_kfp: Callable[..., int] = _behavior_not_defined  # New handler for KFP
) -> int:
    ...
    # Inside fluidos_kubectl_extension function, within the try block:
        if input_format == InputFormat.MSPL:
            logger.info("Invoking MSPL Service Handler")
            return on_mlps(data)
        elif input_format == InputFormat.K8S:
            if _has_intent_defined(spec):
                logger.info("Invoking K8S with Intent Service Handler")
                return on_k8s_w_intent(data)
        elif input_format == InputFormat.KFP:
            logger.info("Invoking Kubeflow Pipeline Handler")
            return on_kfp(data)  # New handler for Kubeflow Pipelines

    # Fallback to kubectl apply if none of the custom handlers match
    logger.info("Invoking kubectl apply")
    return on_apply(argv[1:], stdin_data)

class KubeflowPipelineProcessor:
    """Processor class to handle Kubeflow Pipeline resources."""
    def __init__(self, configuration: ModelBasedOrchestratorConfiguration):
        self._configuration = configuration
        self._k8s_client = client.ApiClient(self._configuration.configuration)

    def __call__(self, data: str | bytes) -> int:
        logger.info("Processing Kubeflow Pipeline request")
        try:
            request = _request_to_dictionary(data)
        except TypeError as e:
            logger.error("Error processing request, possibly malformed")
            logger.debug(f"Error message {e=}")
            return -1

        # Determine the type of Kubeflow resource to create
        kind = request.get("kind")
        if kind not in {"Pipeline", "PipelineRun"}:
            logger.error("Unsupported Kubeflow Pipeline resource type")
            return -1

        try:
            response = client.CustomObjectsApi(self._k8s_client).create_namespaced_custom_object(
                group="kubeflow.org",
                version="v1beta1",
                namespace=self._configuration.namespace,
                plural=kind.lower() + "s",  # Make 'Pipeline' -> 'pipelines', 'PipelineRun' -> 'pipelineruns'
                body=request,
                async_req=False
            )
            logger.info("Kubeflow Pipeline resource created successfully")
        except ApiException as e:
            logger.error(f"Unable to create a Kubeflow Pipeline resource: {e}")
            return -1

        logger.debug(f"Response: {response=}")
        return 0

 def main() -> None:
    raise SystemExit(
        fluidos_kubectl_extension(
            sys.argv,
            sys.stdin,
            on_mlps=lambda x: MSPLProcessor(MSPLProcessorConfiguration.build_configuration(sys.argv))(x),
            on_k8s_w_intent=lambda x: ModelBasedOrchestratorProcessor(ModelBasedOrchestratorConfiguration.build_configuration(sys.argv))(x),
            on_kfp=lambda x: KubeflowPipelineProcessor(ModelBasedOrchestratorConfiguration.build_configuration(sys.argv))(x)  # New handler for KFP
        )
    )


if __name__ == "__main__":
    main()
