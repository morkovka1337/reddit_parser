from re import sub
import praw
import pandas as pd
import multiprocessing as mp
import itertools
from praw.models import MoreComments
import os

from cridentials import client_id, client_secret

CHECK_CORRECTNESS=False
CORE_MULTIPLICATOR = 2

def parse_subreddit_filter(subr, filter):

    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent='Reddit stocks analysis big data 2022',
                         # password='',
                         # username=''
                         )
    subreddit = reddit.subreddit(subr)
    submissions = getattr(subreddit, filter)(limit=None)
    df = pd.DataFrame(columns=['Post created', 'Title', 'Post text', 'Comments'])
    i = 0
    print(f'Processing {subr}.{filter}')
    for submission in submissions:
        submission.comments.replace_more(limit=100)
        for comment in submission.comments:
            if isinstance(comment, MoreComments):
                continue
            comm = comment.list()

            df.loc[i] = [submission.created_utc] + [submission.title] + \
                [submission.selftext] + [comm.body]
            i += 1
    print(f'Saving {subr}_{filter}_with_date.json')
    df.to_json(f'outputs_with_date/{subr}_{filter}_with_date.json')

def check_correctness(subr, filter):

    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent='Reddit stocks analysis big data 2022',
                         # password='',
                         # username=''
                         )
    subreddit = reddit.subreddit(subr)
    submissions = list(getattr(subreddit, filter)(limit=10))
    assert submissions is not None
    print(f'{subr}.{filter} is correct!')

if __name__ == '__main__':


    subreddits = [
        'wallstreetbets',
        'stocks',
        'pennystocks',
        'investing',
        'ValueInvesting',
        'investing_discussion',
        'stonks',
        'shroomstocks',
        'Wallstreetbetsnew',
        'StockMarket',
        'options',
        'RobinHood',
        'RobinHoodPennyStocks',
        'weedstocks',
        'smallstreetbets',
        'SecurityAnalysis',
        'CanadianInvestor',
        'SPACs',
        'InvestmentClub',
        'GreenInvestor',
        'EducatedInvesting',
        'UndervaluedStonks',
        'Canadapennystocks',
        'traders',
        'RichTogether',
        'UnderValuedStocks',
    ]

    filters = [
        'top',
        'hot',
        'controversial',
        'rising',
        'random_rising',
        'gilded',
        'new',
    ]
    subreddits_filters = list(itertools.product(*[subreddits, filters]))
    inputs = [subreddits_filters[i] for i in range(len(subreddits_filters)) if not os.path.exists(f'{subreddits_filters[i][0]}_{subreddits_filters[i][1]}.json')]
    all_inputs = [subreddits_filters[i] for i in range(len(subreddits_filters))]
    if CHECK_CORRECTNESS:
        for elem in subreddits_filters:
            check_correctness(*elem)
    pool = mp.Pool(processes=int(mp.cpu_count()  * CORE_MULTIPLICATOR))
    result = pool.starmap(parse_subreddit_filter, all_inputs)


