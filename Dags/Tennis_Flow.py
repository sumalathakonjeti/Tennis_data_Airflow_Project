import airflow
from airflow import DAG
import os
import papermill as pm
import pandas as pd
from datetime import datetime,timedelta
#import papermill as pm
#from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.python_operator import PythonOperator

# from airflow.executors import
# def read_files():
#     df = pd.read_csv("/Users/suma/Tennis_project/Stats.csv")

def remove_columns_from_stats():
    df = pd.read_csv("/Users/suma/Tennis_project/Stats.csv")
    df.drop(columns=[
        'sets','1', '2', '3', '4', '5', 'pts', 'rank', 'avg_odds', 'max_odds',
        'total_pts', 'service_pts', 'return_pts', 'aces', 'bp_saved', 'bp_faced', 'first_serve_rtn_won', 'second_serve_rtn_won',
        'first_serve_in', 'dbl_faults','first_serve_per'], axis=1, inplace=True)
    df.to_csv("/Users/suma/Tennis_project/Stats_removed.csv")

def remove_columns_from_player():
    df = pd.read_csv("/Users/suma/Tennis_project/Player.csv")
    df.drop(columns=['hand','birthday'], axis=1, inplace=True)
    df.to_csv("/Users/suma/Tennis_project/Player_removed.csv")

def remove_columns_from_match():
    df = pd.read_csv("/Users/suma/Tennis_project/Match.csv")
    df.drop(columns=['round','avg_minutes_game','avg_seconds_point','avg_minutes_set',
                     'year','match_minutes'], axis=1, inplace=True)
    df.to_csv("/Users/suma/Tennis_project/Match_removed.csv")

def merge_files():
    df_player = pd.read_csv("/Users/suma/Tennis_project/Player_removed.csv")
    df_match = pd.read_csv("/Users/suma/Tennis_project/Match_removed.csv")
    df_stats = pd.read_csv("/Users/suma/Tennis_project/Stats_removed.csv")
    merged_match_stats = pd.merge(left=df_match , right=df_stats, left_on='match_id', right_on='match_id')
    merged_all = pd.merge(left=merged_match_stats, right=df_player, left_on='player_id', right_on='player_id')
    merged_all.to_csv("/Users/suma/Tennis_project/Merge_data.csv")

    aus_matches = merged_all[merged_all["tournament"] == 'Australian Open']
    wimb_matches = merged_all[merged_all["tournament"] == 'Wimbledon']
    usopen_matches = merged_all[merged_all["tournament"] == 'US Open']
    french_matches = merged_all[merged_all["tournament"] == 'French Open']
    aus_matches.to_csv("/Users/suma/Tennis_project/AustralianOpen.csv")
    wimb_matches.to_csv("/Users/suma/Tennis_project/Wimbledon.csv")
    usopen_matches.to_csv("/Users/suma/Tennis_project/USopen.csv")
    french_matches.to_csv("/Users/suma/Tennis_project/FrenchOpen.csv")
# def call_jupyter_australian():
#     run_jup_file = PapermillOperator(
#         task_id="1",
#         dag=dag,
#         input_nb='/Users/suma/Tennis_project/Tennis_Australian_Open.ipynb',
#         output_nb='/Users/suma/Tennis_project/Tennis_Aus_Open_outp.ipynb'
#     )
#     run_jup_file
def execute_aus_steps():

    merge_player = pd.read_csv("/Users/suma/Tennis_project/AustralianOpen.csv")
    merge_player['count'] = 0
    merge_player_w = merge_player[merge_player.winner == True]
    merge_player_l = merge_player[merge_player.winner == False]
    aus_win = merge_player_w[['tournament', 'fullname', 'count']]
    aus_lose = merge_player_l[['tournament', 'fullname', 'count']]
    aus_win = aus_win.groupby(['tournament', 'fullname']).count()
    aus_win = aus_win.reset_index()
    aus_win.columns = ['tournament', 'fullname', 'Count_Win']
    aus_lose = aus_lose.groupby(['tournament', 'fullname']).count()
    aus_lose = aus_lose.reset_index()
    aus_lose.columns = ['tournament', 'fullname', 'Count_Lose']
    aus_winner = pd.merge(aus_win, aus_lose, on=['tournament', 'fullname'])
    aus_winner['total_play'] = aus_winner['Count_Win'] + aus_winner['Count_Lose']
    aus_winner['perc_win'] = round(aus_winner['Count_Win'] / aus_winner['total_play'], 4) * 100
    aus_winner = aus_winner[aus_winner.total_play > 5]
    aus_winner.sort_values(by='perc_win', ascending=False).head(100)
    top_aus = aus_winner[aus_winner.tournament == 'Australian Open'].sort_values(by='perc_win', ascending=False).head(
        20)
    aus_winner.to_csv("/Users/suma/Tennis_project/TopAusOpenWin.csv")



def execute_wimb_steps():
    merge_player = pd.read_csv("/Users/suma/Tennis_project/Wimbledon.csv")
    merge_player['count'] = 0
    merge_player_w = merge_player[merge_player.winner == True]
    merge_player_l = merge_player[merge_player.winner == False]
    tour_win = merge_player_w[['tournament', 'fullname', 'count']]
    tour_lose = merge_player_l[['tournament', 'fullname', 'count']]
    tour_win = tour_win.groupby(['tournament', 'fullname']).count()
    tour_win = tour_win.reset_index()
    tour_win.columns = ['tournament', 'fullname', 'Count_Win']
    tour_lose = tour_lose.groupby(['tournament', 'fullname']).count()
    tour_lose = tour_lose.reset_index()
    tour_lose.columns = ['tournament', 'fullname', 'Count_Lose']
    tour_winner = pd.merge(tour_win, tour_lose, on=['tournament', 'fullname'])
    tour_winner['total_play'] = tour_winner['Count_Win'] + tour_winner['Count_Lose']
    tour_winner['perc_win'] = round(tour_winner['Count_Win'] / tour_winner['total_play'], 4) * 100
    tour_winner = tour_winner[tour_winner.total_play > 5]
    tour_winner.sort_values(by='perc_win', ascending=False).head(100)
    top_tour = tour_winner[tour_winner.tournament == 'Wimbledon Open'].sort_values(by='perc_win', ascending=False).head(
        20)
    tour_winner.to_csv("/Users/suma/Tennis_project/TopWimbledon.csv")

def execute_french_steps():
    merge_player = pd.read_csv("/Users/suma/Tennis_project/FrenchOpen.csv")
    merge_player['count'] = 0
    merge_player_w = merge_player[merge_player.winner == True]
    merge_player_l = merge_player[merge_player.winner == False]
    tour_win = merge_player_w[['tournament', 'fullname', 'count']]
    tour_lose = merge_player_l[['tournament', 'fullname', 'count']]
    tour_win = tour_win.groupby(['tournament', 'fullname']).count()
    tour_win = tour_win.reset_index()
    tour_win.columns = ['tournament', 'fullname', 'Count_Win']
    tour_lose = tour_lose.groupby(['tournament', 'fullname']).count()
    tour_lose = tour_lose.reset_index()
    tour_lose.columns = ['tournament', 'fullname', 'Count_Lose']
    tour_winner = pd.merge(tour_win, tour_lose, on=['tournament', 'fullname'])
    tour_winner['total_play'] = tour_winner['Count_Win'] + tour_winner['Count_Lose']
    tour_winner['perc_win'] = round(tour_winner['Count_Win'] / tour_winner['total_play'], 4) * 100
    tour_winner = tour_winner[tour_winner.total_play > 5]
    tour_winner.sort_values(by='perc_win', ascending=False).head(100)
    top_tour = tour_winner[tour_winner.tournament == 'FrenchOpen Open'].sort_values(by='perc_win', ascending=False).head(
        20)
    tour_winner.to_csv("/Users/suma/Tennis_project/TopFrenchOpen.csv")

def execute_usopen_steps():
    merge_player = pd.read_csv("/Users/suma/Tennis_project/USopen.csv")
    merge_player['count'] = 0
    merge_player_w = merge_player[merge_player.winner == True]
    merge_player_l = merge_player[merge_player.winner == False]
    tour_win = merge_player_w[['tournament', 'fullname', 'count']]
    tour_lose = merge_player_l[['tournament', 'fullname', 'count']]
    tour_win = tour_win.groupby(['tournament', 'fullname']).count()
    tour_win = tour_win.reset_index()
    tour_win.columns = ['tournament', 'fullname', 'Count_Win']
    tour_lose = tour_lose.groupby(['tournament', 'fullname']).count()
    tour_lose = tour_lose.reset_index()
    tour_lose.columns = ['tournament', 'fullname', 'Count_Lose']
    tour_winner = pd.merge(tour_win, tour_lose, on=['tournament', 'fullname'])
    tour_winner['total_play'] = tour_winner['Count_Win'] + tour_winner['Count_Lose']
    tour_winner['perc_win'] = round(tour_winner['Count_Win'] / tour_winner['total_play'], 4) * 100
    tour_winner = tour_winner[tour_winner.total_play > 5]
    tour_winner.sort_values(by='perc_win', ascending=False).head(100)
    top_tour = tour_winner[tour_winner.tournament == 'US Open'].sort_values(by='perc_win', ascending=False).head(
        20)
    tour_winner.to_csv("/Users/suma/Tennis_project/TopUSopen.csv")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,11,28),
    # 'email': ['sumalatha.konjeti@gmail.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),

}
# dag = DAG(dag_id='DAG_2',default_args=default_args,catchup=False,schedule_interval='@once')
dag = DAG(
    dag_id='Top_Tennis_Players1',
    default_args=default_args,
    description='ATP data from kaggle',
    # schedule_interval=timedelta(days=1),
)

t1a = PythonOperator(
     task_id='read_and_clean_stats',
     provide_context=False,
     python_callable=remove_columns_from_stats,
     dag=dag,
)

t1b = PythonOperator(
     task_id='read_and_clean_match',
     provide_context=False,
     python_callable=remove_columns_from_match,
     dag=dag,
)

t1c = PythonOperator(
     task_id='read_and_clean_player',
     provide_context=False,
     python_callable=remove_columns_from_player,
     dag=dag,
)

t2 = PythonOperator(
    task_id='merge_files',
    provide_context=False,
    python_callable=merge_files,
    dag=dag,
)

t3a = PythonOperator(
    task_id='process_aus_open',
    provide_context=False,
    python_callable=execute_aus_steps,
    dag=dag,
)

t3b = PythonOperator(
    task_id='process_US_open',
    provide_context=False,
    python_callable=execute_usopen_steps,
    dag=dag,
)

t3c = PythonOperator(
    task_id='process_Wimbledon',
    provide_context=False,
    python_callable=execute_wimb_steps,
    dag=dag,
)

t3d = PythonOperator(
    task_id='process_French_open',
    provide_context=False,
    python_callable=execute_french_steps,
    dag=dag,
)

# t4 = PythonOperator(
#     task_id='process_jupyter_ausopen',
#     provide_context=False,
#     python_callable=call_jupyter_australian,
#     dag=dag,
# )
[t1a,t1b,t1c] >> t2 >> [t3a,t3b,t3c,t3d]