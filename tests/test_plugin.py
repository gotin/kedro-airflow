# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
from pathlib import Path

from kedro.framework.project import _ProjectPipelines
from kedro.pipeline import Pipeline, node

from kedro_airflow.plugin import commands


def identity(arg):
    return arg


def test_create_airflow_dag(mocker, cli_runner, metadata):
    dag_file = Path.cwd() / "airflow_dags" / "hello_world_dag.py"
    default_pipeline = Pipeline(
        [node(identity, ["input"], ["output"]), node(identity, ["output"], ["final"])],
        tags="pipeline",
    )
    _create_pipelines = {
        "__default__": default_pipeline,
    }
    mock_pipelines = mocker.patch.object(
        _ProjectPipelines,
        "_get_pipelines_registry_callable",
        return_value=_create_pipelines,
    )
    mocker.patch("kedro_airflow.plugin.pipelines", mock_pipelines)
    result = cli_runner.invoke(commands, ["airflow", "create"], obj=metadata)
    assert result.exit_code == 0
    assert str(dag_file) in result.output
    assert dag_file.exists()
