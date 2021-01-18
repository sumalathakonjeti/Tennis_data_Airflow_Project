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
    df = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Stats.csv")
    df.drop(columns=[
        'sets','1', '2', '3', '4', '5', 'pts', 'rank', 'avg_odds', 'max_odds',
        'total_pts', 'service_pts', 'return_pts', 'aces', 'bp_saved', 'bp_faced', 'first_serve_rtn_won', 'second_serve_rtn_won',
        'first_serve_in', 'dbl_faults','first_serve_per'], axis=1, inplace=True)
    df.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Stats_removed.csv")

def remove_columns_from_player():
    df = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Player.csv")
    df.drop(columns=['hand','birthday'], axis=1, inplace=True)
    df.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Players_removed.csv")

def remove_columns_from_match():
    df = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Match.csv")
    df.drop(columns=['round','avg_minutes_game','avg_seconds_point','avg_minutes_set',
                     'year','match_minutes'], axis=1, inplace=True)
    df.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Matches_removed.csv")

def merge_files():
    df_player = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Players_removed.csv")
    df_match = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Matches_removed.csv")
    df_stats = pd.read_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Stats_removed.csv")
    merged_match_stats = pd.merge(left=df_match , right=df_stats, left_on='match_id', right_on='match_id')
    merged_all = pd.merge(left=merged_match_stats, right=df_player, left_on='player_id', right_on='player_id')
    merged_all.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/Merged_data.csv")

    aus_matches = merged_all[merged_all["tournament"] == 'Australian Open']
    wimb_matches = merged_all[merged_all["tournament"] == 'Wimbledon']
    usopen_matches = merged_all[merged_all["tournament"] == 'US Open']
    french_matches = merged_all[merged_all["tournament"] == 'French Open']
    aus_matches.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/AustralianOpenData.csv")
    wimb_matches.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/WimbledonData.csv")
    usopen_matches.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/USopenData.csv")
    french_matches.to_csv("/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Data/FrenchOpenData.csv")


def call_jupyter_get_data():
    pm.execute_notebook(
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Get_data.ipynb',
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Get_output_data.ipynb',
        parameters=dict(TEST=True,
                        QUICK_RUN=True, ), )

def call_jupyter_australian():
    pm.execute_notebook(
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_Australian_Open.ipynb',
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_Aus_Open_output.ipynb' ,
        parameters=dict( TEST=True,
                         QUICK_RUN=True,), )

def call_jupyter_wimblendon():
    pm.execute_notebook(
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_wimbledon.ipynb',
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_Wimb_output.ipynb',
        parameters=dict(TEST=True,
                        QUICK_RUN=True, ), )

def call_jupyter_frenchopen():
        pm.execute_notebook(
            '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_french_open.ipynb',
            '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_french_output.ipynb',
            parameters=dict(TEST=True,
                            QUICK_RUN=True, ), )


def call_jupyter_USopen():
    pm.execute_notebook(
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_USopen.ipynb',
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Tennis_US_output.ipynb',
        parameters=dict(TEST=True,
                        QUICK_RUN=True, ), )

def call_jupyter_final_report():
    pm.execute_notebook(
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Final_report.ipynb',
        '/Users/suma/dev/airflow_home/Tennis_data_Airflow_Project/Jupyter_files/Final_report_output.ipynb',
        parameters=dict(TEST=True,
                        QUICK_RUN=True, ), )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,11,28),
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),

}
# dag = DAG(dag_id='DAG_2',default_args=default_args,catchup=False,schedule_interval='@once')
dag = DAG(
    dag_id='Top_Tennis_Players_Final_report1',
    default_args=default_args,
    description='ATP data from kaggle',
    # schedule_interval=timedelta(days=1),
)
t1 = PythonOperator(
     task_id='get_data_from_kaggle',
     provide_context=False,
     python_callable=call_jupyter_get_data,
     dag=dag,
)

t2a = PythonOperator(
     task_id='read_and_clean_stats_data',
     provide_context=False,
     python_callable=remove_columns_from_stats,
     dag=dag,
)

t2b = PythonOperator(
     task_id='read_and_clean_match_data',
     provide_context=False,
     python_callable=remove_columns_from_match,
     dag=dag,
)

t2c = PythonOperator(
     task_id='read_and_clean_player_data',
     provide_context=False,
     python_callable=remove_columns_from_player,
     dag=dag,
)

t3 = PythonOperator(
    task_id='merge_files',
    provide_context=False,
    python_callable=merge_files,
    dag=dag,
)

t4a = PythonOperator(
    task_id='process_jupyter_ausopen',
    provide_context=False,
    python_callable=call_jupyter_australian,
    dag=dag,
)
t4b = PythonOperator(
    task_id='process_jupyter_usopen',
    provide_context=False,
    python_callable=call_jupyter_USopen,
    dag=dag,
)
t4c = PythonOperator(
    task_id='process_jupyter_frenchopen',
    provide_context=False,
    python_callable=call_jupyter_frenchopen,
    dag=dag,
)
t4d = PythonOperator(
    task_id='process_jupyter_wimbledon',
    provide_context=False,
    python_callable=call_jupyter_wimblendon,
    dag=dag,
)
t5 = PythonOperator(
    task_id='Final_report',
    provide_context=False,
    python_callable=call_jupyter_final_report,
    dag=dag,
)
t1 >> [t2a,t2b,t2c] >> t3 >> [t4a,t4b,t4c,t4d] >> t5