#! /usr/bin/env python

# script for testing scan_unmerged_branches

# Standard Imports
import unittest
import pprint
import scan_unmerged_branches
import re
from email.utils import parseaddr
from subprocess import Popen, PIPE
from collections import namedtuple
import string
import datetime
import typing
import tempfile
import os
import sys
import csv
import json

# validation
branch_pattern = r'((feature|hotfix|bugfix|release)/)?([A-Za-z][A-Za-z0-9-_]+)'  # supports BitBucket/GitBranchFlow 
target_branch_regex = re.compile(branch_pattern)
source_branch_regex = re.compile(r'origin/' + branch_pattern)  # TODO: support non origin repo name
hash_regex = re.compile(r'[abcdef0-9]+')
date_frmt = '%Y-%m-%dT%H:%M:%S%z'

# execution
python3exe = sys.executable
this_dir = os.path.dirname(os.path.abspath(__file__))
project_root_dir = os.path.dirname(this_dir)
test_temp_dir = os.path.join(this_dir, 'tmp_test')
os.makedirs(test_temp_dir, exist_ok=True)
scanner = os.path.abspath(scan_unmerged_branches.__file__)
ExecRes = namedtuple('ExecRes', 'rc stdout stderr')


def python_exec(cmd, **kwargs):
    kwargs.setdefault('shell', True)
    kwargs.setdefault('stdout', PIPE)
    kwargs.setdefault('stderr', PIPE)
    proc = Popen(cmd, **kwargs)
    rc = proc.wait()
    res = ExecRes(rc, proc.stdout, proc.stderr)
    return res


def validate_source_branch(branch):
    return bool(source_branch_regex.fullmatch(branch))


def validate_target_branch(branch):
    return bool(target_branch_regex.fullmatch(branch))


def validate_email(email):
    _, address = parseaddr(email)
    if not address:
        return False
    else:
        if address.count('@') != 1:
            return False
        if address.startswith('@'):
            return False
        user_spec, domain_spec = address.split('@')
        if not user_spec or not domain_spec:
            return False
        if '.' not in domain_spec:
            return False
        domain_name, top_level_domain = domain_spec.split('.', 1)
        if not domain_name or not top_level_domain:
            return False
        return True


def validate_hash(hash_):
    return bool(len(hash_) > 6 and hash_regex.fullmatch(hash_))


def validate_subject(subject):
    return bool(len(subject) > 0 and all(c in set(string.printable) - set('\n\r\v\f') for c in subject))


def validate_date(date_):
    try:
        dt = datetime.datetime.strptime(date_, date_frmt)
    except ValueError:
        return False
    else:
        return bool(dt)


def validate_commit(path, commit, errors: typing.Optional[list] = None) -> list:
    errors = [] if errors is None else errors
    if not all(key in commit.keys() for key in ['hash', 'subject', 'date']):
        errors.append(('commit_keys', '{}:{}'.format(path, commit.keys())))
    if not validate_hash(commit['hash']):
        errors.append(('commit_hash', '{}:{}'.format(path, commit['hash'])))
    if not validate_subject(commit['subject']):
        errors.append(('commit_subject', '{}:{}'.format(path, commit['subject'])))
    if not validate_date(commit['date']):
        errors.append(('commit_date', '{}:{}'.format(path, commit['date'])))
    return errors


def match_expected_pattern_by_branch(result: dict):
    """
        {
        "branch": {
            "email": [
                {
                    "hash": <commit sha1>,
                    "subject": <commit message subject>,
                    "date" <commit date>
                }, ...
            ]
        }
    }
    :param result:
    :return:
    """
    errors = []
    for branch, usercommits in result.items():
        if not validate_source_branch(branch):
            errors.append(('branch', branch))
        for email, commits in usercommits.items():
            if not validate_email(email):
                errors.append(('email', '{}:{}'.format(branch, email)))
            for commit in commits:
                validate_commit('{}:{}'.format(branch, email), commit, errors)

    return errors


def match_expected_pattern_by_author(result: dict):
    """
        {
        "email": {
            "repo_dir": {
                "target_branch": [
                    {
                        "hash": <commit sha1>,
                        "subject": <commit message subject>,
                        "date" <commit date>
                    }, ...
                ]
            }
        }
    }
    :param result:
    :return:
    """
    errors = []

    for email, repobranches in result.items():
        if not validate_email(email):
            errors.append(('email', email))
        for repo, branchcommits in repobranches.items():
            # no validation for repo
            for branch, commits in branchcommits.items():
                if not validate_target_branch(branch):
                    errors.append(('branch', '{}:{}:{}'.format(email, repo, branch)))
                for commit in commits:
                    validate_commit('{}:{}:{}'.format(email, repo, branch), commit, errors)

    return errors


class TestRepoBase(unittest.TestCase):

    default_main_branch = 'main'

    @staticmethod
    def init_scanner(**kwargs):
        kwargs.setdefault('save_scans', True)
        sub = scan_unmerged_branches.ScanUnmergedBranches(**kwargs)
        return sub

    def check_result_by_branch(self, result):
        pprint.pprint(result)
        assert isinstance(result, dict)
        errors = match_expected_pattern_by_branch(result)
        self.assertFalse(errors)

    def check_result_by_author(self, result):
        pprint.pprint(result)
        assert isinstance(result, dict)
        errors = match_expected_pattern_by_author(result)
        self.assertFalse(errors)

    def execute_cli_scan(self, branch=None, repo_dir=None, *args):
        output = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='output.', suffix='.json', delete=False)
        options = ' '.join([
            '--output={}'.format(output.name)
        ])
        branch = self.default_main_branch if branch is None else branch
        repo_dir = '.' if repo_dir is None else repo_dir
        args = ' '.join([branch, repo_dir] + list(args))
        cmd = '{python3} {scanner} {options} {args}'.format(
            python3=python3exe,
            scanner=scanner,
            options=options,
            args=args
        )
        print(cmd)
        python_exec(cmd)
        with open(output.name) as f:
            result = json.load(f)
        self.addCleanup(os.unlink, output.name)
        return result

    def execute_code_scan(self, *args, **kwargs):
        kwargs.setdefault('return_report', True)
        return scan_unmerged_branches.scan(*args, **kwargs)

    def execute_cli_scan_multiple(self, configs, mode, output=None, *args, **kwargs):
        assert mode in ('json', 'csv', 'txt')
        if mode == 'json':
            input_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='input.', suffix='.json', delete=False)
            with open(input_file.name, 'w') as f:
                json.dump(configs, f)
        elif mode == 'csv':
            input_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='input.', suffix='.csv', delete=False)
            with open(input_file.name, 'w') as f:
                fieldnames = sorted(set([k for config in configs for k in config.keys()]))
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(configs)
        elif mode == 'txt':
            input_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='input.', suffix='.txt', delete=False)
            with open(input_file.name, 'w') as f:
                f.writelines(['{b} {r}\n'.format(b=config['branch'], r=config['repo_dir']) for config in configs])
        else:
            raise RuntimeError('impossible', mode)
        if output is None:
            # if not provided, we should clean it up
            cleanup_output = True
            output_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='output.', suffix='.json', delete=False)
            output = output_file.name
        else:
            cleanup_output = False
        # compile the command
        options_ = [
            '--input={}'.format(input_file.name),
            '--output={}'.format(output),
        ]
        if kwargs.get('report_by_email'):
            options_.append('--report-by-email')
        if kwargs.get('report_by_repo'):
            options_.append('--report-by-repo')
        if kwargs.get('stale'):
            options_.append('--stale={}'.format(kwargs['stale']))
        args_ = ['--input={}'.format(input_file.name)] + list(args)
        cmd = '{python3} {scanner} {options} {args}'.format(
            python3=python3exe,
            scanner=scanner,
            options=' '.join(options_),
            args=' '.join(args_)
        )
        print(cmd)
        python_exec(cmd)
        with open(output) as f:
            result = json.load(f)
        self.addCleanup(os.unlink, input_file.name)
        if cleanup_output:
            self.addCleanup(os.unlink, output)
        return result

    def execute_code_scan_multiple(self, *args, **kwargs):
        kwargs.setdefault('return_report', True)
        return scan_unmerged_branches.scan_multiple(*args, **kwargs)

    def execute_code_scan_multiple_pipeline(self, *args, **kwargs):
        kwargs.setdefault('return_report', True)
        return scan_unmerged_branches.scan_multiple_pipeline(*args, **kwargs)


class TestThisRepo(TestRepoBase):
    
    MAIN_BRANCH = 'main'
    
    def test_scan_main_branch_defaults(self):
        result = self.execute_code_scan()
        self.check_result_by_branch(result)

    def test_scan_main_branch_explicit_branch(self):
        result = self.execute_code_scan(branch=self.MAIN_BRANCH)
        self.check_result_by_branch(result)

    def test_scan_main_branch_explicit_dir(self):
        result = self.execute_code_scan(repo_dir='.')
        self.check_result_by_branch(result)

    def test_scan_main_branch_cli_defaults(self):
        result = self.execute_cli_scan()
        self.check_result_by_branch(result)

    def test_scan_main_branch_cli_explicit_branch(self):
        result = self.execute_cli_scan(branch=self.MAIN_BRANCH)
        self.check_result_by_branch(result)

    def test_scan_main_branch_cli_explicit_branch_and_repo(self):
        result = self.execute_cli_scan(branch=self.MAIN_BRANCH, repo_dir='.')
        self.check_result_by_branch(result)

    def test_scan_non_existant_branch(self):
        result = self.execute_code_scan(branch='NotExistBranch')
        self.assertFalse(result)

    def test_scan_bad_branch_raises_error(self):
        self.assertRaises(
            AssertionError,
            self.execute_code_scan,
            branch='Bad Branch'
        )


class TestLocalRepo(TestRepoBase):
    
    # TODO: create repo locally before running test and set this somehow
    LOCAL_REPO_DIR = ''
    LOCAL_REPO_MAIN_BRANCH = 'master'
    LOCAL_REPO_DEV_BRANCH = 'development'

    def test_scan_main_branch_default(self):
        result = self.execute_code_scan(repo_dir=self.LOCAL_REPO_DIR)
        self.check_result_by_branch(result)

    def test_scan_main_branch_explicit(self):
        result = self.execute_code_scan(branch=self.LOCAL_REPO_MAIN_BRANCH, repo_dir=self.LOCAL_REPO_DIR)
        self.check_result_by_branch(result)

    def test_scan_development_explicit(self):
        result = self.execute_code_scan(branch=self.LOCAL_REPO_DEV_BRANCH, repo_dir=self.LOCAL_REPO_DIR)
        self.check_result_by_branch(result)

    def test_scan_development_cli_explicit(self):
        result = self.execute_cli_scan(branch=self.LOCAL_REPO_DEV_BRANCH, repo_dir=self.LOCAL_REPO_DIR)
        self.check_result_by_branch(result)

    def test_scan_multiple_main_branch_and_development_by_email(self):
        configs = [
            {'branch': self.LOCAL_REPO_MAIN_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
            {'branch': self.LOCAL_REPO_DEV_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
        ]
        results = self.execute_code_scan_multiple(configs, report_by_email=True)
        self.check_result_by_author(results)

    def test_scan_multiple_main_branch_and_development_by_email_cli_json(self):
        configs = [
            {'branch': self.LOCAL_REPO_MAIN_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
            {'branch': self.LOCAL_REPO_DEV_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
        ]
        results = self.execute_cli_scan_multiple(configs, mode='json', report_by_email=True)
        self.check_result_by_author(results)

    def test_scan_multiple_main_branch_and_development_by_email_cli_csv(self):
        configs = [
            {'branch': self.LOCAL_REPO_MAIN_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
            {'branch': self.LOCAL_REPO_DEV_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
        ]
        results = self.execute_cli_scan_multiple(configs, mode='csv', report_by_email=True)
        self.check_result_by_author(results)

    def test_scan_multiple_main_branch_and_development_by_email_cli_txt(self):
        configs = [
            {'branch': self.LOCAL_REPO_MAIN_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
            {'branch': self.LOCAL_REPO_DEV_BRANCH, 'repo_dir': self.LOCAL_REPO_DIR},
        ]
        results = self.execute_cli_scan_multiple(configs, mode='txt', report_by_email=True)
        self.check_result_by_author(results)


class TestMultipleRepos(TestRepoBase):

    ALL_REPOS = [
        # TODO: populate automatically before running tests with some local repo dirs
    ]
    MAIN_BRANCH = 'master'
    DEV_BRANCH = 'development'

    def test_scan_main_branch_all_repos_one_by_one(self):
        sub = self.init_scanner()
        branch = self.MAIN_BRANCH
        kwargs = {
            'return_report': True
        }
        results = {}
        for repo_dir in self.ALL_REPOS:
            result = sub.scan(branch=branch, repo_dir=repo_dir, **kwargs)
            results[repo_dir] = result
        pprint.pprint(results)
        repo_errors = {repo: match_expected_pattern_by_branch(result) for repo, result in results.items()}
        pprint.pprint(repo_errors)
        repos_with_errors = {repo: errors for repo, errors in repo_errors.items() if errors}
        self.assertFalse(repos_with_errors)
        # check sub.scans
        self.assertEquals(len(self.ALL_REPOS), len(sub.scans))
        for scan in sub.scans:
            self.assertEquals(branch, scan['branch'])
            self.assertIn(scan['repo_dir'], self.ALL_REPOS)
            self.assertDictEqual(kwargs, scan['kwargs'])
            self.assertDictEqual(results[scan['repo_dir']], scan['report'])

    def test_scan_multiple_main_branch_all_repos_by_email(self):
        configs = [{'branch': self.MAIN_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        results = self.execute_code_scan_multiple(configs, report_by_email=True)
        self.check_result_by_author(results)

    def test_scan_multiple_main_branch_all_repos_by_repo(self):
        configs = [{'branch': self.MAIN_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        results = self.execute_code_scan_multiple(configs, report_by_repo=True)
        pprint.pprint(results)

    def test_scan_multiple_main_branch_all_repos_pipeline(self):
        # create input
        input_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='input.', suffix='.json', delete=False)
        input_ = input_file.name
        configs = [
            {'TARGET_BRANCH': self.MAIN_BRANCH, 'REPO_NAME': os.path.basename(repo_dir)} 
            for repo_dir in self.ALL_REPOS
        ]
        with open(input_, 'w') as f:
            json.dump(configs, f)
        # prepare output path
        output_file = tempfile.NamedTemporaryFile(dir=test_temp_dir, prefix='output.', suffix='.json', delete=False)
        output = output_file.name
        # execute
        results = self.execute_code_scan_multiple_pipeline(input_, output, workspace='c:/dev')
        # results
        pprint.pprint(results)
        with open(output) as f:
            pipeline_results = json.load(f)
        pprint.pprint(pipeline_results)

    def test_scan_multiple_main_branch_all_repos(self):
        configs = [{'branch': self.MAIN_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        results = self.execute_code_scan_multiple(configs)
        pprint.pprint(results)
        repo_errors = {result['repo_dir']: match_expected_pattern_by_branch(result['report']) for result in results}
        pprint.pprint(repo_errors)
        repos_with_errors = {repo: errors for repo, errors in repo_errors.items() if errors}
        self.assertFalse(repos_with_errors)

    def test_scan_multiple_main_branch_all_repos_by_email_cli_json(self):
        configs = [{'branch': self.MAIN_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        results = self.execute_cli_scan_multiple(configs, mode='json', report_by_email=True)
        self.check_result_by_author(results)

    def test_scan_multiple_main_branch_and_development_all_repos_by_email(self):
        configs = [{'branch': self.MAIN_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        configs += [{'branch': self.DEV_BRANCH, 'repo_dir': repo_dir} for repo_dir in self.ALL_REPOS]
        results = self.execute_code_scan_multiple(configs, report_by_email=True)
        self.check_result_by_author(results)


class TestValidators(unittest.TestCase):

    def test_branch_valid(self):
        # main branches
        self.assertTrue(validate_source_branch('origin/main'))
        self.assertTrue(validate_source_branch('origin/development'))
        # branch types
        self.assertTrue(validate_source_branch('origin/feature/branch'))
        self.assertTrue(validate_source_branch('origin/release/branch'))
        self.assertTrue(validate_source_branch('origin/bugfix/branch'))
        self.assertTrue(validate_source_branch('origin/hotfix/branch'))
        # legal chars
        self.assertTrue(validate_source_branch('origin/underscore_'))
        self.assertTrue(validate_source_branch('origin/dash-'))

    def test_branch_invalid(self):
        # wrongly formatted branches
        self.assertFalse(validate_source_branch('main'))
        self.assertFalse(validate_source_branch('development'))
        # illegal branches
        self.assertFalse(validate_source_branch('origin/branch with spaces'))
        self.assertFalse(validate_source_branch('origin/9branchStartNumber'))
        self.assertFalse(validate_source_branch('origin/unsupported/branchType'))
        bad_chars = set(string.punctuation) - set('-_')
        for char in bad_chars:
            self.assertFalse(validate_source_branch('origin/branch_with_bad_char_{char}'.format(char=char)))

    def test_email_valid(self):
        self.assertTrue(validate_email('some-email@domain.com'))

    def test_email_invalid(self):
        # bad emails
        self.assertFalse(validate_email('noAtSign.com'))
        self.assertFalse(validate_email('@only.domain'))
        self.assertFalse(validate_email('bad@Domain'))
        self.assertFalse(validate_email('OnlyAddress@'))
        self.assertFalse(validate_email(''))
        self.assertFalse(validate_email('@.'))

    def test_hash_valid(self):
        # real hash
        self.assertTrue(validate_hash('3f43932c33423fb9521e84ac8350b24499930e69'))
        self.assertTrue(validate_hash('df8d77d5b0868f17dbd89cfb639de73fde4bdb90'))
        self.assertTrue(validate_hash('4c8d084c69b9c1a8410ee6868820761c32619451'))

    def test_hash_invalid(self):
        self.assertFalse(validate_hash('BADCHARACTERSUPPER'))
        self.assertFalse(validate_hash('badcharacterslower'))
        self.assertFalse(validate_hash(''))

    def test_subject_valid(self):
        # real subject
        self.assertTrue(validate_subject('scan unmerged branches script with tests'))
        self.assertTrue(validate_subject('x'))
        self.assertTrue(validate_subject('has \t tab character'))

    def test_subject_invalid(self):
        self.assertFalse(validate_subject(''))
        self.assertFalse(validate_subject('has \n new line'))
        self.assertFalse(validate_subject('has \r carriage return'))
        self.assertFalse(validate_subject('has \v vertical tab'))
        self.assertFalse(validate_subject('has \f form feed'))

    def test_date_valid(self):
        # real date
        self.assertTrue(validate_date('2020-01-28T11:39:03+02:00'))

    def test_date_invalid(self):
        self.assertFalse(validate_date(''))
        self.assertFalse(validate_date('2020-01-28'))
        self.assertFalse(validate_date('11:39:03'))
        self.assertFalse(validate_date('2020-01-28T11:39:03'))


if __name__ == '__main__':
    unittest.main()
