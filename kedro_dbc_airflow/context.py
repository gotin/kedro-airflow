# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Entry point for running a Kedro pipeline as a Python package."""
from pathlib import Path
from typing import Any, Dict, Union

from kedro.framework.context import KedroContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


class ProjectContext(KedroContext):
    """A subclass of KedroContext to add Spark initialisation for the pipeline.

    [Notice!] This is updated after being generated by kedro command to set the
    spark session up after KedroSession creation. Because databricks-connect
    setting is needed via airflow's connection which connection_id is defined by
    kedro's configuration.
    """

    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(package_name, project_path, env, extra_params)

    def init_spark_session(self, additional_conf: Dict[str, str]) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        additional_conf = additional_conf if additional_conf is not None else {}
        parameters = self.config_loader.get("spark*", "spark*/**")
        parameters.update(additional_conf)
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        _spark_session = (
            SparkSession.builder.appName(self.package_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("WARN")