import os

import click
import git
import requests

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPOSITORY = git.Repo(ROOT_DIR)


def send_github_event(workflow_name: str, token: str, inputs=()):
    if inputs is None:
        inputs = {}
    payload = {"ref": "ci", "inputs": inputs}
    headers = {"Authorization": f"token {token}",
               "Accept": "application/vnd.github.v3+json"}
    response = requests.post(
        f"https://api.github.com/repos/it4innovations/rsds/actions/workflows/{workflow_name}/dispatches",
        json=payload,
        headers=headers)
    response.raise_for_status()


token_option = click.option("-t", "--token", required=True, envvar="IR_GITHUB_TOKEN", show_envvar=True,
                            help="GitHub token. Note, it should have `repo` scope.")


@click.command(help="Create a new release")
@click.argument("branch")
@click.argument("tag")
@token_option
def make_release(branch: str, tag: str, token: str):
    """
    Create a release using GitHub.

    BRANCH is either a branch, commit or SHA from which to create the release.
    TAG is the name of the release.
    TOKEN is a Github token.
    """
    sha = REPOSITORY.rev_parse(branch).hexsha
    send_github_event("release.yml", token, {
        "sha": sha,
        "tag": tag
    })


if __name__ == '__main__':
    make_release()
