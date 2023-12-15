#!/usr/bin/env python3
#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from urllib.parse import urljoin
import argparse
import os
import requests
import re
import subprocess
import sys
import textwrap


class github_api_client:
    def __init__(self, github_api_url, owner, project):
        github_user = os.getenv("GITHUB_LOGIN")
        github_token = os.getenv("GITHUB_TOKEN")

        owner = "scylladb"
        project = "scylladb"

        self.base_url = urljoin(github_api_url, f"/repos/{owner}/{project}/placeholder")

        self._session = requests.Session()
        self._session.auth = (github_user, github_token)

    def _abspath(self, path):
        if path.startswith('https://'):
            return path
        return urljoin(self.base_url, path)

    def get(self, path, params=None):
        return self._session.get(self._abspath(path), params=params).json()

    def paged_get(self, path, params=None):
        path = self._abspath(path)
        if params is None:
            params = {}
        params["page"] = 1
        page_content = self._session.get(path, params=params).json()
        while page_content:
            for elem in page_content:
                yield elem
            params["page"] += 1

            page_content = self._session.get(path, params=params).json()


def _get_branches(git_branch_list):
    branches = git_branch_list.strip().split('\n')
    if not branches:
        return []
    return [branch.lstrip().split('/')[1] for branch in branches if branch]


def _abbrev_commit(commit):
    if commit is None:
        return None
    return subprocess.check_output(["git", "show", "-s", "--pretty=format:%h", commit], text=True).strip()


def find_stale_backport_candidates(
        api_client: github_api_client,
        remote: str = "origin",
        limit: int = -1,
        skip_branch: str = None):

    subprocess.check_call(["git", "fetch", "-q", remote, "--recurse-submodules=no"])
    branches = _get_branches(subprocess.check_output(["git", "branch", "-rl", f"{remote}/branch-*"], text=True))
    if not branches:
        raise RuntimeError(f"Failed to fetch branches from {remote}")
    if skip_branch is not None:
        branches = [branch for branch in branches if branch != skip_branch]
    live_branches = branches[-2:]

    for branch in ["master"] + live_branches:
        subprocess.check_call(["git", "fetch", "-q", remote, "--recurse-submodules=no", branch])

    cherry_pick_re = re.compile("\\(cherry picked from commit ([0-9a-f]+)\\)")

    count = 0
    for issue in [ api_client.get("issues/12104") ]:
    #for issue in api_client.paged_get("issues", params={"labels": "Backport candidate", "state": "closed"}):
        merged = False
        closing_commit = None
        referencing_commits = []
        backports = dict()
        for event in api_client.paged_get(issue["events_url"]):
            if event["event"] == "referenced" and event["commit_id"] is not None:
                referencing_commits.append(event["commit_id"])
            elif event["event"] == "closed" and (
                    event["commit_id"] is not None or event["actor"]["login"] == "scylladb-promoter"):
                merged = True
                # We want the last close event, in case there is multiple (think revert + reopen + reclose)
                closing_commit = event["commit_id"]

        fixes_re = re.compile("[Ff]ixes:? .*{}".format(issue["number"]))

        for commit in referencing_commits:
            try:
                containing_branches = _get_branches(
                        subprocess.check_output(["git", "branch", "-q", "--remote", "--contains", commit],
                                                text=True, stderr=subprocess.STDOUT))
            except subprocess.CalledProcessError:
                continue  # Commit doesn't exist

            commit_description = subprocess.check_output(["git", "show", "-s", "--pretty=format:%b", commit],
                                                         text=True)

            print(commit)

            if re.search(fixes_re, commit_description) is None:
                continue  # commit only references, not fixes

            print(f"{commit} - {containing_branches}")

            if 'master' in containing_branches:
                if closing_commit is None:
                    closing_commit = commit

                backport_match = re.search(cherry_pick_re, commit_description)

                # Either the closing commit itself, or a backport of it
                if backport_match is None or backport_match[1] == closing_commit:
                    containing_live_branches = set(containing_branches) & set(live_branches)
                    for branch in containing_live_branches:
                        backports[branch] = _abbrev_commit(commit)

        if not merged:
            continue

        print("{} closed-by: {} backports: {}".format(
            issue["html_url"],
            _abbrev_commit(closing_commit),
            backports))

        count += 1
        if count == limit:
            break


class RawDescriptionDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass


def main():
    parser = argparse.ArgumentParser(
            prog=sys.argv[0],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                maintainer.py

                Helper script for maintainers, automatic the most dull and repeating tasks.
            """))
    parser.add_argument("-r", "--remote", action="store", type=str, default="origin",
                        help="Git remote to use")

    subparsers = parser.add_subparsers(dest="operation", help="the operation to execute")

    parser_fsbc = subparsers.add_parser(
            "find-stale-backport-candidates",
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                    Find stale backport candidates

                    Find issues that are:
                    * closed
                    * have the "Backport candidate" label
                    * were closed by a commit
                    * either:
                        * the closing commit is in the last two OSS branches
                        * the closing commit + backports already cover the last two OSS branches
            """))
    parser_fsbc.add_argument("-l", "--limit", action="store", type=int, default=-1,
                             help="Stop after listing this many issues, -1 stands for no limit")
    parser_fsbc.add_argument("--skip-branch", action="store", type=str,
                             help="Skip this branch when considering backports")

    args = parser.parse_args()

    # FIXME: don't hardcode owner and repo
    api_client = github_api_client("https://api.github.com", "scylladb", "scylladb")

    operation_name = args.operation.replace('-', '_')
    args_dict = vars(args)
    del args_dict["operation"]
    globals()[operation_name](api_client, **args_dict)


if __name__ == '__main__':
    main()
