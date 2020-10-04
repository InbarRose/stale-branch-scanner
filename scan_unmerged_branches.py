#! /usr/bin/env python

# script for scanning repositories and finding branches that have changes which are not merged (to master)

# Standard Imports
from subprocess import Popen, PIPE
from optparse import OptionParser
from collections import namedtuple
import os
import csv
import sys
import json
import string
import datetime

ExecRes = namedtuple('ExecRes', 'rc stdout stderr')
DEFAULT_MAIN_BRANCH = 'main'


def git_exec(cmd, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    kwargs.setdefault('shell', True)
    kwargs.setdefault('text', True)
    kwargs.setdefault('stdout', PIPE)
    kwargs.setdefault('stderr', PIPE)
    proc = Popen(cmd, **kwargs)
    stdout, stderr = proc.communicate(timeout=timeout)
    rc = proc.returncode
    res = ExecRes(rc, stdout.splitlines(), stderr.splitlines())
    return res


class ScanUnmergedBranches(object):
    COMMIT_DETAILS = namedtuple('COMMIT_DETAILS', ['hash', 'date', 'author', 'subject'])
    DATE_FRMT = '%Y-%m-%dT%H:%M:%S%z'
    STALE_DAYS_DEFAULT = '7'
    default_main_branch = DEFAULT_MAIN_BRANCH

    @staticmethod
    def assert_no_whitespace(text, message=None):
        message = message or text
        assert not any(c in string.whitespace for c in text), message

    def __init__(self, **kwargs):
        self.save_scans = kwargs.pop('save_scans', False)
        self.scans = []
        self.main_branch_name = kwargs.pop('main_branch_name', self.default_main_branch)
        super().__init__()

    def scan(self, branch=None, repo_dir='.', **kwargs):
        branch = branch or self.main_branch_name
        # verify args
        self.assert_no_whitespace(branch, 'branch:{}'.format(branch))
        self.assert_no_whitespace(repo_dir, 'repo_dir:{}'.format(repo_dir))
        # extract kwargs
        scan_kwargs = kwargs.copy()
        return_report = kwargs.pop('return_report', False)
        include_main = kwargs.pop('include_main', False)
        fetch_first = kwargs.pop('fetch_first', True)
        save_scan = kwargs.pop('save_scan', self.save_scans)
        stale = int(kwargs.pop('stale', self.STALE_DAYS_DEFAULT))
        # perform git fetch if needed
        if fetch_first:
            self.execute_git_fetch(repo_dir)
        # scan unmerged branches
        unmerged_branches = self.get_list_of_unmerged_branches(branch, repo_dir, include_main=include_main)
        # fetch unmerged commits for branches
        unmerged_commits_by_branch = self.fetch_unmerged_commits_by_branch(unmerged_branches, branch, repo_dir)
        # get staleness
        stale_branches_with_commits = self.extract_stale_branches(unmerged_commits_by_branch, stale)
        # create report
        report_by_branch = self.create_report_by_branch(stale_branches_with_commits)
        # save scan
        if save_scan:
            self.scans.append(
                {'branch': branch, 'repo_dir': repo_dir, 'report': report_by_branch, 'kwargs': scan_kwargs})
        # return
        if return_report:
            return report_by_branch
        else:
            return self.write_report(report_by_branch, **kwargs)

    def fetch_unmerged_commits_by_branch(self, unmerged_branches, branch=None, repo_dir='.'):
        branch = branch or self.main_branch_name
        # verify args
        self.assert_no_whitespace(branch, 'branch:{}'.format(branch))
        self.assert_no_whitespace(repo_dir, 'repo_dir:{}'.format(repo_dir))
        # create report
        unmerged_commits_by_branch = {}
        for unmerged_branch in unmerged_branches:
            unmerged_branch_commits = self.get_list_of_unmerged_commits(unmerged_branch, branch, repo_dir)
            unmerged_commits_by_branch[unmerged_branch] = unmerged_branch_commits
        return unmerged_commits_by_branch

    def create_report_by_branch(self, stale_branches_with_commits):
        report_by_branch = {}
        for branch, commits in stale_branches_with_commits.items():
            commits_by_author = self.convert_commits_list_to_dict_by_author(commits)
            report_by_branch[branch] = commits_by_author
        return report_by_branch

    def extract_stale_branches(self, unmerged_commits_by_branch, stale):
        stale_branches_with_commits = {}
        for branch, commits in unmerged_commits_by_branch.items():
            dates = [commit.date for commit in commits]
            if not all(self.date_is_older_than_n_days(d, stale) for d in dates):
                continue  # not stale do not add to stale branches list
            stale_branches_with_commits[branch] = commits
        return stale_branches_with_commits

    @classmethod
    def extract_latest_date_from_commits(cls, commits):
        return max([datetime.datetime.strptime(commit['date'], cls.DATE_FRMT) for commit in commits])

    @classmethod
    def date_is_older_than_n_days(cls, date_, n_days, raise_exceptions=False):
        try:
            dt = datetime.datetime.strptime(date_, cls.DATE_FRMT)
        except ValueError as exc:
            if raise_exceptions:
                raise exc
            return False
        else:
            return bool((cls.get_datetime_now_with_tz() - dt).days >= n_days)

    @classmethod
    def get_datetime_now_with_tz(cls):
        return datetime.datetime.now().astimezone()

    @staticmethod
    def print_json_to_stdout(json_data, **json_kwargs):
        print(json.dumps(json_data, **json_kwargs))

    @staticmethod
    def save_json_to_file(json_data, file_path, **json_kwargs):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(json_data, f, **json_kwargs)
        print('report saved to file: {}'.format(file_path))

    def write_report(self, report, **kwargs):
        raise_exceptions = kwargs.pop('raise_exceptions', True)
        output = kwargs.pop('output', None) or None
        indent_ = kwargs.pop('indent', 4)

        try:
            if output is None:
                self.print_json_to_stdout(report, indent=indent_)
            else:
                self.save_json_to_file(report, output, indent=indent_)
        except Exception as exc:
            print('exception saving report: {}'.format(exc))
            if raise_exceptions:
                raise exc
            return 1
        else:
            return 0

    def write_pipeline_report(self, report, output, **kwargs):
        raise_exceptions = kwargs.pop('raise_exceptions', True)
        indent_ = kwargs.pop('indent', 4)

        pipeline_report = self.create_pipeline_report(report)

        try:
            self.save_json_to_file(pipeline_report, output, indent=indent_)
        except Exception as exc:
            print('exception saving report: {}'.format(exc))
            if raise_exceptions:
                raise exc
            return 1
        else:
            return 0

    def execute_git_fetch(self, repo_dir='.', **kwargs):
        self.assert_no_whitespace(repo_dir, 'repo_dir:{}'.format(repo_dir))
        repo_dir = os.path.abspath(repo_dir)
        kwargs.setdefault('cwd', repo_dir)
        # build command
        options = ['--prune', '--prune-tags', '--no-tags', '--no-recurse-submodules', '--unshallow']
        cmd = 'git -P fetch {options}'.format(options=' '.join(options))
        # executed
        res = git_exec(cmd, **kwargs)
        if '--unshallow on a complete repository does not make sense' in res.stderr:
            options.remove('--unshallow')
            cmd = 'git -P fetch {options}'.format(options=' '.join(options))
            res = git_exec(cmd, **kwargs)
        return res

    def get_list_of_unmerged_branches(self, branch=None, repo_dir='.', **kwargs) -> list:
        branch = branch or self.main_branch_name
        self.assert_no_whitespace(branch, 'target_branch:{}'.format(branch))
        self.assert_no_whitespace(repo_dir, 'repo_dir:{}'.format(repo_dir))
        repo_dir = os.path.abspath(repo_dir)
        include_main = kwargs.pop('include_main', False)
        kwargs.setdefault('cwd', repo_dir)
        if not branch.startswith('origin/'):
            branch = 'origin/{}'.format(branch)
        # build command
        cmd = 'git -P branch -r --no-merged {}'.format(branch)
        # executed
        res = git_exec(cmd, **kwargs)
        # make list of branches
        branches = [branch.strip() for branch in res.stdout]

        def branch_filter(b):
            if any(c in string.whitespace for c in b):
                return False
            if include_main is False and b == f'origin/{self.main_branch_name}':
                return False
            return True

        branches = list(filter(branch_filter, branches))
        return branches

    def get_list_of_unmerged_commits(self, source_branch, target_branch, repo_dir='.', **kwargs) -> list:
        self.assert_no_whitespace(source_branch, 'source_branch:{}'.format(source_branch))
        self.assert_no_whitespace(target_branch, 'target_branch:{}'.format(target_branch))
        self.assert_no_whitespace(repo_dir, 'repo_dir:{}'.format(repo_dir))
        repo_dir = os.path.abspath(repo_dir)
        kwargs.setdefault('cwd', repo_dir)
        if not target_branch.startswith('origin/'):
            target_branch = 'origin/{}'.format(target_branch)
        if not source_branch.startswith('origin/'):
            source_branch = 'origin/{}'.format(source_branch)
        # build command
        cmd = 'git -P log {} --not {} --format="%H|%aI|%aE|%s"'.format(source_branch, target_branch)
        # executed
        res = git_exec(cmd, **kwargs)
        # make list of commit author:hash:subject:date
        commits = [self.COMMIT_DETAILS(*commit.strip().split('|')) for commit in res.stdout]
        return commits

    def convert_commits_list_to_dict_by_author(self, commits) -> dict:
        # convert commits from list [(hash, date, author, subject), ...]
        #                 to   dict {author: [{hash: hash, date:date, subject:subject}, ...], ...}
        dict_by_author = {}
        for commit in commits:
            commit_dict = self.convert_commit_to_dict(commit)
            author = commit_dict.pop('author')  # remove author from commit dictionary and use as key
            author_commit_list = dict_by_author.setdefault(author, list())
            author_commit_list.append(commit_dict)
        return dict_by_author

    @staticmethod
    def convert_commit_to_dict(commit: COMMIT_DETAILS):
        commit_dict = dict(commit._asdict())
        return commit_dict

    def scan_multiple_pipeline(self, configs, pipeline_output, **kwargs):
        # handle kwargs before sending to scan (some of them should not be sent, or we want to ensure certain kwargs)
        kwargs['return_report'] = True  # override in order to always get scan report from self.scan
        workspace = kwargs.pop('workspace', os.getenv('WORKSPACE'))  # jenkins default uses "WORKSPACE"

        results_by_branch = []
        for config in configs:
            # each config MUST have TARGET_BRANCH & REPO_NAME
            branch = config.pop('TARGET_BRANCH')
            repo_name = config.pop('REPO_NAME')  # we assume all repos are in the same root dir so name will suffice
            repo_dir = os.path.join(workspace, repo_name)
            # then we take the rest of config as kwargs, and **kwargs overwrite
            scan_kwargs = config
            scan_kwargs.update(kwargs)
            report_by_branch = self.scan(branch, repo_dir, **scan_kwargs)
            results_by_branch.append(
                {'branch': branch, 'repo_dir': repo_dir, 'report': report_by_branch, 'kwargs': scan_kwargs})

        self.write_pipeline_report(results_by_branch, pipeline_output)

        return 0

    def scan_multiple(self, configs, **kwargs):
        # handle kwargs before sending to scan (some of them should not be sent, or we want to ensure certain kwargs)
        return_report = kwargs.pop('return_report', False)
        kwargs['return_report'] = True  # override in order to always get scan report from self.scan
        report_by_email = kwargs.pop('report_by_email', False)
        report_by_repo = kwargs.pop('report_by_repo', False)
        output = kwargs.pop('output', None)

        results_by_branch = []
        for config in configs:
            # each config MUST have branch & repo_dir
            branch = config.pop('branch')
            repo_dir = config.pop('repo_dir')
            # then we take the rest of config as kwargs, and **kwargs overwrite
            scan_kwargs = config
            scan_kwargs.update(kwargs)
            report_by_branch = self.scan(branch, repo_dir, **scan_kwargs)
            results_by_branch.append(
                {'branch': branch, 'repo_dir': repo_dir, 'report': report_by_branch, 'kwargs': scan_kwargs})

        if report_by_repo:
            report = self.aggregate_scan_results_by_repo(results_by_branch)
        elif report_by_email:
            report = self.aggregate_scan_results_by_email(results_by_branch)
        else:
            report = results_by_branch

        if return_report:
            return report
        else:
            return self.write_report(report, output=output, **kwargs)

    @staticmethod
    def read_configs(configs_path):
        """read configs from file for multiple scanning"""
        if configs_path.endswith('.json'):
            with open(configs_path) as f:
                configs = json.load(f)
        elif configs_path.endswith('.csv'):
            with open(configs_path) as f:
                reader = csv.DictReader(f)
                configs = [line for line in reader]
        else:
            # assume text file with whitespace delimiter and only BRANCH and REPO_DIR as args
            with open(configs_path) as f:
                lines = filter(None, f.readlines())
                configs = [dict(zip(['branch', 'repo_dir'], line.split())) for line in lines]

        return configs

    @staticmethod
    def read_configs_pipeline(configs_path):
        """read configs from file for multiple scanning"""
        assert configs_path.endswith('.json')
        with open(configs_path) as f:
            configs = json.load(f)
        return configs

    @classmethod
    def create_pipeline_report(cls, results_by_branch):
        pipeline_results = {'scans': {}}
        message_lines = []  # message for sending to slack
        for result_by_branch in sorted(results_by_branch, key=lambda r: (os.path.basename(r['repo_dir']), r['branch'])):
            repo_dir = result_by_branch['repo_dir']
            repo_name = os.path.basename(repo_dir)  # we assume the repo name is unique
            target_branch = result_by_branch['branch']
            scan_id = '<{}>:{}'.format(repo_name, target_branch)

            if not result_by_branch['report']:
                # fresh repo, add it to the message so we know it was scanned, and mark is as fresh.
                message_lines.append(' - {} *fresh*'.format(scan_id))
                continue  # if it is fresh, we don't need to collect branches or add it to the list of stale results

            # for stale repo we should collect branches etc.
            pipeline_results['scans'][scan_id] = result_by_branch['report']
            message_lines.append(' - {}'.format(scan_id))
            for branch, commits_by_author in result_by_branch['report'].items():
                for author, commits_list in commits_by_author.items():
                    # we want to add how stale the branch is by getting the staleness of the most recent commit
                    latest_date = cls.extract_latest_date_from_commits(commits_list)
                    staleness = (cls.get_datetime_now_with_tz() - latest_date).days
                    message_lines.append('    * {} ({} days)'.format(branch, staleness))

        pipeline_results['message'] = '\n'.join(message_lines)
        return pipeline_results

    @staticmethod
    def aggregate_scan_results_by_repo(results_by_branch):
        results_by_repo = {}
        for result_by_branch in results_by_branch:
            repo_dir = result_by_branch['repo_dir']
            target_branch = result_by_branch['branch']
            repo_dict = results_by_repo.setdefault(repo_dir, {})
            repo_dict[target_branch] = result_by_branch['report']
        return results_by_repo

    @staticmethod
    def aggregate_scan_results_by_email(results_by_branch):
        results_by_email = {}
        for result_by_branch in results_by_branch:
            repo_dir = result_by_branch['repo_dir']
            target_branch = result_by_branch['branch']
            for branch, commits_by_author in result_by_branch['report'].items():
                for author, commits_list in commits_by_author.items():
                    author_dict = results_by_email.setdefault(author, {})
                    repo_dict = author_dict.setdefault(repo_dir, {})
                    repo_dict[target_branch] = commits_list
        return results_by_email


def scan(branch, repo_dir='.', **kwargs):
    sub = ScanUnmergedBranches()
    return sub.scan(branch, repo_dir, **kwargs)


def scan_multiple(configs, **kwargs):
    sub = ScanUnmergedBranches()
    return sub.scan_multiple(configs, **kwargs)


def scan_multiple_from_input_file(input_file, **kwargs):
    sub = ScanUnmergedBranches()
    configs = sub.read_configs(input_file)
    return sub.scan_multiple(configs, **kwargs)


def scan_multiple_pipeline(pipeline_input_file, pipeline_output_file, **kwargs):
    sub = ScanUnmergedBranches()
    configs = sub.read_configs_pipeline(pipeline_input_file)
    return sub.scan_multiple_pipeline(configs, pipeline_output_file, **kwargs)


usage = """%prog [options] [BRANCH] [REPO_DIR]

if no BRANCH is provided, uses the default main branch (main)
if no REPO_DIR is provided, uses .

if not --output path is provided, print to STDOUT

if --input file_path is provided, do not read BRANCH or REPO_DIR from arguments,
instead use input file_path as configuration (.json, .csv, or .txt file allowed)
Input file Modes:
    .json :
        file is json file with an array/list as root object. each item in the list is a config
        config MUST have branch and repo_dir values, and can define additional supported options 
    .csv :
        file is csv file with head_row. each row is a config
        config MUST have branch and repo_dir values, and can define additional supported options
        rows that do not define an option should leave it blank (empty string)
    .txt :
        file is a txt file. each line is a a config
        config is BRANCH and REPO_DIR separated by whitespace, can define no additional options
        
    supported options (json and csv mode only): include_main, stale, fetch_first

"""


def main(args):
    parser = OptionParser(usage=usage)
    parser.add_option('--input', dest='input_file', default='',
                      help='(optional) the path for the input file (MUST be one of [.json, .csv or .txt])')
    parser.add_option('--output', dest='output', default='',
                      help='(optional) the path for the output file (MUST be a .json file)')
    parser.add_option('--pipeline-input', dest='pipeline_input', default='',
                      help='(for pipeline only) read pipeline format input from path (MUST be a .json file)')
    parser.add_option('--pipeline-output', dest='pipeline_output', default='',
                      help='(for pipeline only) write pipeline format output to path (MUST be a .json file)')
    parser.add_option('--include-main', dest='include_main', default=False, action="store_true",
                      help='Include main branch when checking unmerged commits (relevant when BRANCH is not main)')
    parser.add_option('--no-fetch-first', dest='fetch_first', default=True, action="store_false",
                      help='Do not perform git fetch before scanning (usually you want to fetch first)')
    parser.add_option('--stale', dest='stale', default=ScanUnmergedBranches.STALE_DAYS_DEFAULT,
                      help='How many days without changes to consider a branch stale (default 7)')
    parser.add_option('--report-by-email', dest='report_by_email', default=False, action="store_true",
                      help='(with input file only) report will be aggregated by author email (default: none)')
    parser.add_option('--report-by-repo', dest='report_by_repo', default=False, action="store_true",
                      help='(with input file only) report will be aggregated by repo (default: none)')
    options, args = parser.parse_args(args)

    kwargs = {}

    if options.output and not options.output.endswith('.json'):
        parser.error('output path must be a .json file')

    kwargs.setdefault('output', options.output)
    kwargs.setdefault('include_main', options.include_main)
    kwargs.setdefault('fetch_first', options.fetch_first)
    kwargs.setdefault('stale', options.stale)
    kwargs.setdefault('report_by_email', options.report_by_email)
    kwargs.setdefault('report_by_repo', options.report_by_repo)

    if options.pipeline_input and options.pipeline_output:
        # pipeline scan
        return scan_multiple_pipeline(options.pipeline_input, options.pipeline_output, **kwargs)
    elif options.input_file:
        # multiple scan
        return scan_multiple_from_input_file(options.input_file, **kwargs)
    else:
        # single scan
        branch = DEFAULT_MAIN_BRANCH
        repo_dir = '.'

        if not args:
            pass  # using default branch and repo_dir
        elif len(args) == 1:
            branch = args[0]
        elif len(args) == 2:
            branch = args[0]
            repo_dir = args[1]
        else:
            parser.error('too many arguments: count={} args={}'.format(len(args), args))

        return scan(branch, repo_dir, **kwargs)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
