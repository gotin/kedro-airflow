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
""" Kedro plugin for running a project with Databricks-connect on Airflow """
import os
import shutil
from collections import defaultdict
from distutils.dir_util import copy_tree
from pathlib import Path

import click
import jinja2
from click import secho
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata
from slugify import slugify

CONTEXT_FOR_DBC_AIRFLOW_FILEPATH = os.path.join(
    str(Path(__file__).parent), "context.py")


@click.group(name="DBCAirflow")
def commands():
    """ Kedro plugin for running a project with Databricks-connect & Airflow """
    pass


@commands.group(name="dbcaf")
def dbcaf_commands():
    """Run project with Databricks-connect & Airflow"""
    pass


@dbcaf_commands.command()
@click.option("-p", "--pipeline", "pipeline_name", default="__default__")
@click.option("-e", "--env", default="local")
@click.option(
    "-t",
    "--target-dir",
    "target_path",
    type=click.Path(writable=True, resolve_path=True, file_okay=False),
    default="./airflow_dags/",
)
@click.option(
    "-j",
    "--template-file",
    "template_file",
    default="dbc_af_dag_template.j2",
)
@click.pass_obj
def create(
    metadata: ProjectMetadata, pipeline_name, env, target_path, template_file,
):  # pylint: disable=too-many-locals
    """Create an Airflow DAG for a project"""
    template_path = str(Path(__file__).parent)
    loader = jinja2.FileSystemLoader(template_path)
    print(f"{template_path=}")

    jinja_env = jinja2.Environment(
        autoescape=True,
        loader=loader,
        lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(template_file)

    project_path = metadata.project_path
    package_name = metadata.package_name
    dag_filename = f"{package_name}_dag.py"

    target_path = Path(target_path)
    target_dag_filepath = target_path / dag_filename

    target_dag_filepath.parent.mkdir(parents=True, exist_ok=True)
    with KedroSession.create(package_name, project_path, env=env) as session:
        context = session.load_context()
        pipeline = context.pipelines.get(pipeline_name)

    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)

    template.stream(
        dag_name=package_name,
        dependencies=dependencies,
        env=env,
        pipeline_name=pipeline_name,
        package_name=package_name,
        pipeline=pipeline,
    ).dump(str(target_dag_filepath))

    # package name directory in dags directory
    dags_kedro_conf_dirpath = os.path.join(
        target_path, "kedro_conf", package_name)
    os.makedirs(dags_kedro_conf_dirpath, exist_ok=True)

    dags_logs_dirpath = os.path.join(dags_kedro_conf_dirpath, "logs")
    os.makedirs(dags_logs_dirpath, exist_ok=True)

    # conf directory
    dags_conf_dirpath = os.path.join(dags_kedro_conf_dirpath, "conf")
    os.makedirs(dags_conf_dirpath, exist_ok=True)
    copy_tree("conf", dags_conf_dirpath)

    # python source code files
    dags_package_dirpath = os.path.join(target_path, package_name)
    os.makedirs(dags_package_dirpath, exist_ok=True)
    package_dirpath = os.path.join("src", package_name)
    for path in Path(package_dirpath).glob("**/**"):
        if path.is_dir() and not str(path).startswith("__"):
            src_path = str(path)
            dest_path = os.path.join(
                src_path.replace(
                    package_dirpath,
                    dags_package_dirpath))
            os.makedirs(dest_path, exist_ok=True)
    src_context_path = os.path.join(package_dirpath, "context.py")
    for path in Path(package_dirpath).glob("**/*.py"):
        src_path = str(path)
        dest_path = os.path.join(
            src_path.replace(
                package_dirpath,
                dags_package_dirpath))
        if src_path == src_context_path:
            shutil.copy(CONTEXT_FOR_DBC_AIRFLOW_FILEPATH, dest_path)
        else:
            shutil.copy(src_path, dest_path)
    context_filepath = f""

    secho("")
    secho("An Airflow DAG has been generated in:", fg="green")
    secho(str(target_dag_filepath))
    secho("This file should be copied to your Airflow DAG folder.", fg="yellow")
    secho(
        "The Airflow configuration can be customized by editing this file.", fg="green"
    )
    secho("")
    secho(
        "This file also contains the path to the config directory, this directory will need to "
        "be available to Airflow and any workers.",
        fg="yellow",
    )
    secho("")
    secho(
        "Additionally all data sets must have an entry in the data catalog.",
        fg="yellow",
    )
    secho(
        "And all local paths in both the data catalog and log config must be absolute paths.",
        fg="yellow",
    )
    secho("")
