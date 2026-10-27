import airflow
import datetime
import urllib.request as request
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

second_dag = DAG(
    dag_id='second_dag_stub',
    default_args=default_args_dict,
    schedule_interval=None,  
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)
def get_random_user(epoch,output_folder):
    request.urlretrieve(url='https://randomuser.me/api/',filename=f'{output_folder}/{epoch}_user..json',)
task_one = PythonOperator(task_id='get_random_user', dag=second_dag,python_callable=get_random_user, op_kwargs={"output_folder":"/opt/airflow/dags","epoch":"{{execution_date.int_timestamp}}",},)

def check_gender(epoch,output_folder):
    with open(f'{output_folder}/{epoch}_user..json','r') as f:
        data=json.load(f)
    gender=data['results'][0]['gender']
    if gender == 'female':
        return "process_female"
    else:
        return "process_male"
    
task_two = BranchPythonOperator(task_id='check_gender', dag=second_dag,python_callable=check_gender,op_kwargs={"output_folder":"/opt/airflow/dags","epoch":"{{execution_date.int_timestamp}}",},)

def _process_user(table, epoch, output_folder):
    with open(f'{output_folder}/{epoch}_user..json','r') as f:
        user=json.load(f)['results'][0]
    fisrtname=user['name']['first'].replace("'", "''")
    lastname=user['name']['last'].replace("'", "''")
    gender=user['gender']
    email=user['email']
    country=user['location']['country']

    sql=f"""Create table if not exists {table} (first_name TEXT, last_name TEXT, gender TEXT, email TEXT, country TEXT);
            insert into {table} values('{fisrtname}','{lastname}','{gender}','{email}','{country}'); """
    with open (f"{output_folder}/insert_{table}.sql","w") as t:
        t.write(sql)    


task_three_a = PythonOperator(task_id='process_female', dag=second_dag,python_callable=_process_user,op_kwargs={"table": "users_female","output_folder":"/opt/airflow/dags","epoch":"{{execution_date.int_timestamp}}",},)

task_three_b = PythonOperator(task_id='process_male', dag=second_dag,python_callable=_process_user,op_kwargs={"table": "users_male","output_folder":"/opt/airflow/dags","epoch":"{{execution_date.int_timestamp}}",},)
task_four_a = PostgresOperator(task_id='insert_female', dag=second_dag,postgres_conn_id='postgres_not_default',sql='insert_users_female.sql',)

task_four_b = PostgresOperator(task_id='insert_male', dag=second_dag,postgres_conn_id='postgres_not_default', sql='insert_users_male.sql',)

join_tasks = DummyOperator(
    task_id='join_tasks',
    dag=second_dag,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end',
    dag=second_dag,
    trigger_rule='none_failed'
)
task_one >> task_two
task_two >> [task_three_a,task_three_b]
task_three_a >> task_four_a >> join_tasks
task_three_b >> task_four_b >> join_tasks
join_tasks >> end

# TODO 1: (Tùy chọn) Nghiên cứu và thay thế các DummyOperator ở trên bằng các PythonOperator, BranchPythonOperator, PostgresOperator tương ứng.
# Nhiệm vụ: Tải dữ liệu JSON từ https://randomuser.me/api/, đọc trường 'gender', rẽ nhánh tạo file SQL tương ứng và chèn vào database.

# TODO 2: Khai báo DAG Dependencies (mối quan hệ luồng đi của các task)
# Gợi ý luồng đi:
# task_one >> task_two
# task_two >> [task_three_a, task_three_b] (Rẽ nhánh)
# Nhánh A: task_three_a >> task_four_a >> join_tasks
# Nhánh B: task_three_b >> task_four_b >> join_tasks
# Cuối cùng: join_tasks >> end
pass
