# Github REST API v3: https://developer.github.com/v3/

import os
import logging
import requests
import json
import datetime
import dateutil
import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
from urllib.parse import urlparse

GITHUB_API_BASEURL = "https://api.github.com"

class GithubRepoStats:
    def __init__(self, repo_url: str = None, github_username: str = None, github_token: str = None):
        if not repo_url:
            raise ValueError("Parameter 'repo_url' required")
        
        self.repo_url = repo_url

        # Get repo owner and name
        (self.owner, self.repo) = GithubRepoStats.get_owner_repo_from_url(self.repo_url)
        if not (self.owner and self.repo):
            raise ValueError(f'Invalid repository url: {self.repo_url}')

        # Initialise request session with authentication
        self.gh_session = requests.Session()
        self.gh_session.auth = (
            github_username or os.environ.get('GITHUB_USERNAME'),
            github_token or os.environ.get('GITHUB_TOKEN'),
        )

    def commit_stats(self):
        commits_array = self.commits_of_repo(days_before=365)
        # print(json.dumps(commits_array, indent=4, sort_keys=True))
        # return
        commits = pd.DataFrame(pd.json_normalize(commits_array))
        # print('Columns:', commits.columns)
        # print('Index:', commits.index)

        commits['date'] =  pd.to_datetime(commits['commit.committer.date'])
        commits['date'] =  pd.to_datetime(commits['date'], utc=True)
        commits['commit_date'] = commits['date'].dt.date
        commits['commit_week'] = commits['date'].dt.isocalendar().week
        commits['commit_hour'] = commits['date'].dt.hour
        commits['commit_month'] = commits['date'].dt.month
        commits['commit_year'] = commits['date'].dt.year
        # drop unnecessary columns
        commits = commits[['sha', 'author.login', 'commit_date', 'commit_hour', 'commit_month', 'commit_year']]
        # print(commits.head())

        ## Summary
        ## number of commits
        num_commits = commits['sha'].size
        print('num_commits: ', num_commits)
        ## Average commits per day
        avg_commits_per_day = num_commits / 365
        print('avg_commits_per_day: ', avg_commits_per_day)
        ## number of contributors
        num_contributors = commits['author.login'].unique().size
        print('num_contributors: ', num_contributors)

        # Commits by hour
        # commits_by_hour = commits.groupby('commit_hour')[['sha']].count()
        # commits_by_hour = commits_by_hour.rename(columns = {'sha': 'commit_count'})
        # print('commits_by_hour: ', commits_by_hour)

    def branch_stats(self):
        branches_array = self.branches_of_repo()
        branches = pd.DataFrame(pd.json_normalize(branches_array), columns=['name', 'commit', 'protected'])
        # print(branches)
        # print('Columns:', branches.columns)
        # print('Index:', branches.index)

    def branches_of_repo(self):
        branches = []
        next = True
        i = 1
        while next == True:
            url = GITHUB_API_BASEURL + '/repos/{}/{}/branches?page={}&per_page=100'.format(self.owner, self.repo, i)
            branch_pg = self.gh_session.get(url = url)
            branch_pg_list = branch_pg.json()
            # branch_pg_list = [dict(item, **{'repo_name':'{}'.format(self.repo)}) for item in branch_pg_list]
            # branch_pg_list = [dict(item, **{'owner':'{}'.format(self.owner)}) for item in branch_pg_list]
            branches = branches + branch_pg_list
            if 'Link' in branch_pg.headers:
                if 'rel="next"' not in branch_pg.headers['Link']:
                    next = False
            i = i + 1
        
        return branches

    def commits_of_repo(self, days_before = 0):
        if not days_before:
            days_before = 365
            # since_date = datetime.datetime.strptime(since, '%Y-%m-%d %H:%M:%S.%f')
        # since_date = datetime.datetime.now() - datetime.timedelta(days=(days_before))
        commits = []
        next = True
        i = 1
        while next == True:
            url = GITHUB_API_BASEURL + '/repos/{}/{}/commits?page={}&per_page=100'.format(self.owner, self.repo, i)
            commit_pg = self.gh_session.get(url = url)
            commit_pg_list = commit_pg.json()
            # commit_pg_list = [dict(item, **{'repo_name':'{}'.format(self.repo)}) for item in commit_pg_list]    
            # commit_pg_list = [dict(item, **{'owner':'{}'.format(self.owner)}) for item in commit_pg_list]
            commits = commits + commit_pg_list
            
            # Exit if no more data received
            if (len(commit_pg_list) == 0):
                return commits
            
            if 'Link' in commit_pg.headers and 'rel="next"' not in commit_pg.headers['Link']:
                return commits

            # Return if days_before exceeded
            # Calculate last commit received date: 'commit.committer.date'
            last_commit_date_string = commit_pg_list[-1].get('commit', {}).get('committer', {}).get('date');
            last_commit_date = dateutil.parser.parse(last_commit_date_string)
            timedelta = datetime.datetime.now(datetime.timezone.utc) - last_commit_date
            if timedelta.days > days_before:
                return commits
            
            i = i + 1

        return commits

    # ## 3. Pull Requests
    def pulls_of_repo(self):
        pulls = []
        next = True
        i = 1
        while next == True:
            url = GITHUB_API_BASEURL + '/repos/{}/{}/pulls?page={}&per_page=100'.format(self.owner, self.repo, i)
            pull_pg = self.gh_session.get(url = url)
            pull_pg_list = [dict(item, **{'repo_name':'{}'.format(self.repo)}) for item in pull_pg.json()]    
            pull_pg_list = [dict(item, **{'owner':'{}'.format(self.owner)}) for item in pull_pg_list]
            pulls = pulls + pull_pg_list
            if 'Link' in pull_pg.headers:
                if 'rel="next"' not in pull_pg.headers['Link']:
                    next = False
            i = i + 1
        return pulls

    def issues_of_repo(self):
        issues = []
        next = True
        i = 1
        while next == True:
            url = GITHUB_API_BASEURL + '/repos/{}/{}/issues?page={}&per_page=100'.format(self.owner, self.repo, i)
            issue_pg = self.gh_session.get(url = url)
            issue_pg_list = [dict(item, **{'repo_name':'{}'.format(self.repo)}) for item in issue_pg.json()]    
            issue_pg_list = [dict(item, **{'owner':'{}'.format(self.owner)}) for item in issue_pg_list]
            issues = issues + issue_pg_list
            if 'Link' in issue_pg.headers:
                if 'rel="next"' not in issue_pg.headers['Link']:
                    next = False
            i = i + 1
        return issues
    
    @classmethod
    def get_owner_repo_from_url(cls, url):
        try:
            parsed_url = urlparse(url)
            url_path_parts = parsed_url.path.split('/')
            return (url_path_parts[1], url_path_parts[2])
        except Exception:
            print(f'Error parsing url: {url}')
            return (None, None)

    # function that converts all object columns to strings, in order to store them efficiently into the database
    @classmethod
    def objects_to_strings(cls, table):
        measurer = np.vectorize(len)
        df_object = table.select_dtypes(include=[object])
        string_columns = dict(zip(df_object, measurer(
            df_object.values.astype(str)).max(axis=0)))
        string_columns = {key: String(length=value) if value > 0 else String(length=1)
                        for key, value in string_columns.items() }
        return string_columns
