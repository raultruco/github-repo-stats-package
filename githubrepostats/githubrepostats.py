# Github REST API v3: https://developer.github.com/v3/

import os
import logging
import requests
import json
import datetime
import dateutil
import pytz
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

    def commit_stats(self, days_before:int = 30):
        commits_array = self.fetch_repo_commits(days_before=days_before)
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
        print(commits)

        ## Stats
        stats = {
            'num_commits': commits['sha'].size,
            'avg_commits_per_day':  commits['sha'].size / (days_before + 1),   # Also include today
            'num_contributors': commits['author.login'].unique().size,
        }
        # print('stats: ', stats)
        return stats
    
    def metric_stats(self):
        add_del_list = self.fetch_weekly_aditions_deletions_activity_of_repo()
        commit_list = self.fetch_weekly_commits_activity_of_repo()
        top_contributor_list = self.fetch_commit_contributors_activity_of_repo()

        # print('add_del_list ********* ')
        # print(json.dumps(add_del_list, indent=4))
        # print('commit_list ********** ')
        # print(json.dumps(commit_list, indent=4))
        # return

        add_del_list_df = pd.DataFrame(add_del_list, columns = ['last_updated', 'additions', 'deletions'])
        add_del_list_df['deletions'] = add_del_list_df['deletions'].abs()
        add_del_list_df['last_updated'] =  pd.to_datetime(add_del_list_df['last_updated'], unit='s')
        add_del_list_df.set_index('last_updated')
        # print('add_del_list_df ********* ')
        # print(add_del_list_df)

        commit_list_df = pd.DataFrame(commit_list, columns = ['week', 'total'])
        commit_list_df.rename(columns={'week': 'last_updated', 'total': 'commits'}, inplace=True)
        commit_list_df['last_updated'] =  pd.to_datetime(commit_list_df['last_updated'], unit='s')
        commit_list_df.set_index('last_updated')
        # print('commit_list_df ********* ')
        # print(commit_list_df)

        weekly_top_contributor_list = []
        for contributor in top_contributor_list:
            for week in contributor['weeks']:
                additions_int = int(week['a'])
                deletions_int = int(week['d'])
                commits_int = int(week['c'])
                if (contributor['author']['type'] == 'User' and commits_int > 0):
                    weekly_top_contributor_list.append({'last_updated': week['w'], 'top_contributors': 1, 'top_contributors_additions': additions_int, 'top_contributors_deletions': deletions_int, 'top_contributors_commits':commits_int })

        weekly_top_contributors_df = pd.DataFrame(weekly_top_contributor_list, columns = ['last_updated', 'top_contributors', 'top_contributors_additions', 'top_contributors_deletions', 'top_contributors_commits'])
        weekly_top_contributors_df['last_updated'] =  pd.to_datetime(weekly_top_contributors_df['last_updated'], unit='s')
        weekly_top_contributors_df.set_index('last_updated')
        groups = weekly_top_contributors_df.groupby('last_updated')
        grouped_weekly_top_contributors_df = groups.agg({'top_contributors': 'sum', 'top_contributors_additions' : 'sum', 'top_contributors_deletions' : 'sum', 'top_contributors_commits' : 'sum'})
        
        # print('grouped_weekly_top_contributors_df ********* ')
        # print(grouped_weekly_top_contributors_df.tail(200))
        # return

        metrics_df = pd.merge(commit_list_df, add_del_list_df, on='last_updated', how='left')
        metrics_df = pd.merge(metrics_df, grouped_weekly_top_contributors_df, on='last_updated', how='left')
        # print('metrics_df ********* ')
        # print(metrics_df)

        # Time series:  https://www.dataquest.io/blog/tutorial-time-series-analysis-with-pandas/
        # print('----------------- ')
        current_metrics_df = metrics_df.iloc[-1:, :]
        previous_metrics_df= metrics_df.iloc[:-1, :]
        # print(current_metrics_df)
        # print(previous_metrics_df)

        last_month_means = np.mean(previous_metrics_df.tail(4), axis=0)
        last_3_months_means = np.mean(previous_metrics_df.tail(12), axis=0)
        last_6_months_means = np.mean(previous_metrics_df.tail(24), axis=0)
        last_year_means = np.mean(previous_metrics_df, axis=0)
        # print('last_month_means: \n', last_month_means)
        # print('last_3_months_means: \n', last_3_months_means)
        # print('last_6_months_means: \n', last_6_months_means)
        # print('last_year_means: \n', last_year_means)

        # print('last_month_mean_commits_diff: ', current_metrics_df['commits'].iloc[0] - last_month_means['commits'])
        # print('last_3_months_mean_commits_diff: ', current_metrics_df['commits'].iloc[0] - last_3_months_means['commits'])
        # print('last_6_months_mean_commits_diff: ', current_metrics_df['commits'].iloc[0] - last_6_months_means['commits'])
        # print('last_year_mean_commits_diff: ', current_metrics_df['commits'].iloc[0] - last_year_means['commits'])

        ## Metric stats
        stats = {
            'commits': int(current_metrics_df['commits'].iloc[0]),
            'aditions': int(current_metrics_df['additions'].iloc[0]),
            'deletions': int(current_metrics_df['deletions'].iloc[0]),
            'last_updated': current_metrics_df['last_updated'].iloc[0],
            'commits_diff1': int(current_metrics_df['commits'].iloc[0] - last_month_means['commits']),
            'commits_diff3': int(current_metrics_df['commits'].iloc[0] - last_3_months_means['commits']),
            'commits_diff6': int(current_metrics_df['commits'].iloc[0] - last_6_months_means['commits']),
            'commits_diff12': int(current_metrics_df['commits'].iloc[0] - last_year_means['commits']),
            'additions_diff1': int(current_metrics_df['additions'].iloc[0] - last_month_means['additions']),
            'additions_diff3': int(current_metrics_df['additions'].iloc[0] - last_3_months_means['additions']),
            'additions_diff6': int(current_metrics_df['additions'].iloc[0] - last_6_months_means['additions']),
            'additions_diff12': int(current_metrics_df['additions'].iloc[0] - last_year_means['additions']),
            'deletions_diff1': int(current_metrics_df['deletions'].iloc[0] - last_month_means['deletions']),
            'deletions_diff3': int(current_metrics_df['deletions'].iloc[0] - last_3_months_means['deletions']),
            'deletions_diff6': int(current_metrics_df['deletions'].iloc[0] - last_6_months_means['deletions']),
            'deletions_diff12': int(current_metrics_df['deletions'].iloc[0] - last_year_means['deletions']),
        }
        # print('metric stats ********* ')
        # print(json.dumps(stats, indent=4))
        return stats

    # def metric_rankings(self):
    #     add_del_list = self.fetch_weekly_aditions_deletions_activity_of_repo()
    #     commit_list = self.fetch_weekly_commits_activity_of_repo()
    #     top_contributor_list = self.fetch_commit_contributors_activity_of_repo()

    #     # print('add_del_list ********* ')
    #     # print(json.dumps(add_del_list, indent=4))
    #     # print('commit_list ********** ')
    #     # print(json.dumps(commit_list, indent=4))
    #     # return

    #     add_del_list_df = pd.DataFrame(add_del_list, columns = ['last_updated', 'additions', 'deletions'])
    #     add_del_list_df['deletions'] = add_del_list_df['deletions'].abs()
    #     add_del_list_df['last_updated'] =  pd.to_datetime(add_del_list_df['last_updated'], unit='s')
    #     add_del_list_df.set_index('last_updated')
    #     # print('add_del_list_df ********* ')
    #     # print(add_del_list_df)

    #     commit_list_df = pd.DataFrame(commit_list, columns = ['week', 'total'])
    #     commit_list_df.rename(columns={'week': 'last_updated', 'total': 'commits'}, inplace=True)
    #     commit_list_df['last_updated'] =  pd.to_datetime(commit_list_df['last_updated'], unit='s')
    #     commit_list_df.set_index('last_updated')
    #     # print('commit_list_df ********* ')
    #     # print(commit_list_df)

    #     weekly_top_contributor_list = []
    #     for contributor in top_contributor_list:
    #         for week in contributor['weeks']:
    #             additions_int = int(week['a'])
    #             deletions_int = int(week['d'])
    #             commits_int = int(week['c'])
    #             if (contributor['author']['type'] == 'User' and commits_int > 0):
    #                 weekly_top_contributor_list.append({'last_updated': week['w'], 'top_contributors': 1, 'top_contributors_additions': additions_int, 'top_contributors_deletions': deletions_int, 'top_contributors_commits':commits_int })

    #     weekly_top_contributors_df = pd.DataFrame(weekly_top_contributor_list, columns = ['last_updated', 'top_contributors', 'top_contributors_additions', 'top_contributors_deletions', 'top_contributors_commits'])
    #     weekly_top_contributors_df['last_updated'] =  pd.to_datetime(weekly_top_contributors_df['last_updated'], unit='s')
    #     weekly_top_contributors_df.set_index('last_updated')
    #     groups = weekly_top_contributors_df.groupby('last_updated')
    #     grouped_weekly_top_contributors_df = groups.agg({'top_contributors': 'sum', 'top_contributors_additions' : 'sum', 'top_contributors_deletions' : 'sum', 'top_contributors_commits' : 'sum'})
        
    #     # print('grouped_weekly_top_contributors_df ********* ')
    #     # print(grouped_weekly_top_contributors_df.tail(200))
    #     # return

    #     metrics_df = pd.merge(commit_list_df, add_del_list_df, on='last_updated', how='left')
    #     metrics_df = pd.merge(metrics_df, grouped_weekly_top_contributors_df, on='last_updated', how='left')
    #     print('metrics_df ********* ')
    #     print(metrics_df)


    # def branch_stats(self):
    #     branches_array = self.fetch_repo_branches()
    #     branches = pd.DataFrame(pd.json_normalize(branches_array), columns=['name', 'commit', 'protected'])
    #     # print(branches)
    #     # print('Columns:', branches.columns)
    #     # print('Index:', branches.index)

    def fetch_repo_branches(self):
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

    def fetch_repo_commits(self, days_before: int = 90):
        days_before = days_before if days_before > -1 else 0

        since_date = datetime.datetime.now(tz=pytz.utc).today() - datetime.timedelta(days=(days_before))
        since_date_iso = GithubRepoStats._format_datetime(datetime.datetime.combine(since_date, datetime.time.min))

        commits = []
        next = True
        i = 1
        while next == True:
            url = GITHUB_API_BASEURL + '/repos/{}/{}/commits?since={}&page={}&per_page=100'.format(self.owner, self.repo, since_date_iso, i)
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
            
            i = i + 1

        return commits

    def fetch_repo_pulls(self):
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

    def fetch_repo_issues(self):
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

    # https://docs.github.com/en/rest/reference/metrics#get-the-weekly-commit-activity
    def fetch_weekly_aditions_deletions_activity_of_repo(self):
        url = GITHUB_API_BASEURL + '/repos/{}/{}/stats/code_frequency'.format(self.owner, self.repo)
        metrics = self.gh_session.get(url = url)
        metrics_list = metrics.json()
        return metrics_list
    
    # https://docs.github.com/en/rest/reference/metrics#get-the-last-year-of-commit-activity
    def fetch_weekly_commits_activity_of_repo(self):
        url = GITHUB_API_BASEURL + '/repos/{}/{}/stats/commit_activity'.format(self.owner, self.repo)
        metrics = self.gh_session.get(url = url)
        metrics_list = metrics.json()
        return metrics_list

    # https://docs.github.com/en/rest/reference/metrics#get-all-contributor-commit-activity
    def fetch_commit_contributors_activity_of_repo(self):
        url = GITHUB_API_BASEURL + '/repos/{}/{}/stats/contributors'.format(self.owner, self.repo)
        metrics = self.gh_session.get(url = url)
        metrics_list = metrics.json()
        return metrics_list

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
    
    @classmethod
    def _format_datetime(cls, dt):
        if isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ') 
        return str(dt)
