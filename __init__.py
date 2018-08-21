"""

    Github Plugin

    This plugin provides an interface to the Github v3 API.

    The Github Hook extends the HttpHook and accepts both Basic and
    Token Authentication. If both are available, the hook will use
    the specified token, which should be in the following format in
    the extras field: {"token":"XXXXXXXXXXXXXXXXXXXXX"}

        The host value in the Hook should contain the following:
        https://api.github.com/

    The Github Operator provides support for the following endpoints:
        Comments
        Commits
        Commit Comments
        Issue Comments
        Issues
        Members
        Organizations
        Pull Requests
        Repositories

"""

from airflow.plugins_manager import AirflowPlugin
from github_plugin.hooks.github_hook import GithubHook
from github_plugin.operators.github_to_cloud_storage_operator import GithubToCloudStorageOperator


class GithubPlugin(AirflowPlugin):
    name = "github_plugin"
    operators = [GithubToCloudStorageOperator]
    hooks = [GithubHook]
