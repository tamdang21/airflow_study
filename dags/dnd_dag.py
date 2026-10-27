from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests,random
from faker import Faker
fake = Faker()
# TODO: Thêm các thư viện cần thiết như requests, faker, random

default_args = {
    'owner': 'student',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


def generate_base_chars(**kwargs):
    """PythonOperator 1: Khởi tạo 5 nhân vật cơ bản"""
    nhan_vat=[]
    for i in range (1,6):
        nv = {
            "name":fake.name().replace("'","''"),
            "attributes":str([random.randint(6, 18)] + [random.randint(2, 18) for _ in range(5)]),
            "level": random.randint(1,3) }
        nhan_vat.append(nv)

    
    # TODO: Dùng faker tạo ra 5 nhân vật, mỗi nhân vật có name, attributes (random 6 chỉ số), level (1-3)
    # TODO: Đẩy list các nhân vật này lên XCom
    return nhan_vat
def fetch_races_and_classes(**kwargs):
    """PythonOperator 2: Gọi API lấy race và class cho từng nhân vật"""
    # TODO: Lấy list nhân vật từ XCom
    # TODO: Gọi API https://www.dnd5eapi.co/api/races và https://www.dnd5eapi.co/api/classes để lấy dữ liệu
    # TODO: Cập nhật thông tin race, class cho 5 nhân vật và đẩy lại lên XCom
    # TODO: Nếu gọi API thành công, return "SUCCESS", nếu lỗi, return "FAILED"
    ti = kwargs["ti"]
    nhan_vat= ti.xcom_pull(task_ids="generate_base_chars")
    try:
        response1=requests.get("https://www.dnd5eapi.co/api/2014/races",timeout=10)
        
        response2=requests.get("https://www.dnd5eapi.co/api/2014/classes",timeout=10)
        response1.raise_for_status()
        response2.raise_for_status()
    
        data=response1.json()['results']
        data1=response2.json()['results']
        races=[items['name'] for items in data]
        classes=[items['name'] for items in data1]
        for i in nhan_vat:
            i['race']=random.choice(races)
            i['class']=random.choice(classes)
        ti.xcom_push(key='nhan_vat_update',value=nhan_vat)
        return "SUCCESS"    
    except Exception as e:
        print(f'Lỗi khi gọi api {type(e).__name__}:{e}')
        return "FAILED"

    

def check_api_success(**kwargs):
    """BranchPythonOperator 1: Rẽ nhánh luồng chạy dựa trên kết quả gọi API"""
    ti = kwargs["ti"]
    ket_qua=ti.xcom_pull(task_ids='fetch_races_and_classes')
    if ket_qua == 'SUCCESS':
        return 'fetch_spells_and_profs'
    # TODO: Lấy kết quả return (SUCCESS/FAILED) từ task fetch_races_and_classes qua XCom
    # TODO: Return tên task tiếp theo tương ứng (tiếp tục sinh dữ liệu hoặc đi vào nhánh báo lỗi)
    else:
        return 'log_error'

def fetch_spells_and_profs(**kwargs):
    """PythonOperator 3: Lấy spells và chuẩn bị dữ liệu nhân vật"""
    ti = kwargs["ti"]

    # Lấy danh sách nhân vật đã có race và class
    nhan_vat = ti.xcom_pull(
        task_ids="fetch_races_and_classes",
        key="nhan_vat_update"
    )

    if not nhan_vat:
        raise ValueError("Không lấy được danh sách nhân vật từ XCom")
    # Lấy danh sách languages
    response = requests.get(
        "https://www.dnd5eapi.co/api/2014/languages",
        timeout=10
    )
    response.raise_for_status()
    languages = [
        item["name"] for item in response.json()["results"]
    ]

    # Lấy danh sách proficiencies
    response = requests.get(
        "https://www.dnd5eapi.co/api/2014/proficiencies",
        timeout=10
    )
    response.raise_for_status()
    proficiencies = [
        item["name"] for item in response.json()["results"]
    ]

    for nv in nhan_vat:
        try:
            # Chọn ngẫu nhiên languages và proficiencies
            nv["languages"] = str(random.choice(languages))
            nv["profficiency_choices"] = str(random.choice(proficiencies))            
            class_index = nv["class"].lower().replace(" ", "-")

            # Lấy danh sách spells của class
            spells_url = (
                f"https://www.dnd5eapi.co/api/2014/classes/"
                f"{class_index}/spells"
            )

            response = requests.get(spells_url, timeout=10)
            response.raise_for_status()

            spell_data = response.json()

            if spell_data.get("count", 0) == 0:
                nv["spells"] = "none"
                continue

            spell_results = spell_data.get("results", [])

            # Lọc phép có level <= 2 ngay từ dữ liệu API trả về
            valid_spells = [
                spell["name"]
                for spell in spell_results
                if spell["level"] <= 2
            ]

            if not valid_spells:
                nv["spells"] = "none"
            else:
                so_luong = min(nv["level"] + 3, len(valid_spells))
                nv["spells"] = str(random.sample(valid_spells, so_luong))
            
        except Exception as e:
            print(
                f"Lỗi lấy spells cho class {nv.get('class')}: {e}"
            )
            raise

    # Đẩy danh sách đã cập nhật lên XCom
    ti.xcom_push(key="nhan_vat_update", value=nhan_vat)

    return nhan_vat

def log_error(**kwargs):
    """PythonOperator 4: Ghi log lỗi"""
    print("API Failed. Cannot generate characters.")

with DAG('dnd_weekly_characters', default_args=default_args, 
         schedule_interval='@weekly', catchup=False,) as dag:
    
    start = DummyOperator(task_id="start")

    end = DummyOperator(
            task_id="end",
            trigger_rule="none_failed"
        )


    task_one=PythonOperator(
        task_id="generate_base_chars",
        python_callable=generate_base_chars,
    
    )
    task_two= PythonOperator(
        task_id="fetch_races_and_classes",
        python_callable=fetch_races_and_classes
    )

    # TODO: Khai báo 4 PythonOperator gắn với các hàm python_callable ở trên.
    
    # TODO: Khai báo 1 BranchPythonOperator.
    task_three=BranchPythonOperator(
        task_id="check_api_success",
        python_callable=check_api_success

    )
    task_four=PythonOperator(
        task_id="fetch_spells_and_profs",
        python_callable=fetch_spells_and_profs
    )
    task_five=PythonOperator(
        task_id="log_error",
        python_callable=log_error
    )
    # TODO: Khai báo 1 PostgresOperator (postgres_default) để tạo bảng dnd_characters (nếu chưa có) và dùng Jinja để INSERT dữ liệu.
    insert_nhan_vat=PostgresOperator(
        task_id='insert_nhan_vat',
        postgres_conn_id="postgres_not_default",
        sql=""" CREATE TABLE IF NOT EXISTS NHAN_VAT (name TEXT,attributes TEXT,level INT,race TEXT,class TEXT,languages TEXT, profficiency_choices TEXT,spells TEXT);
        {%set nhan_vat = ti.xcom_pull(task_ids='fetch_spells_and_profs',key='nhan_vat_update')%}
        {%for nv in nhan_vat%}
        INSERT INTO NHAN_VAT (name,attributes,level,race,class,languages, profficiency_choices,spells)
         Values('{{nv["name"]|replace("'", "''")}}','{{nv["attributes"]|replace("'", "''")}}',
         {{nv["level"]}},'{{nv["race"]|replace("'", "''")}}',
         '{{nv["class"]|replace("'", "''")}}','{{nv["languages"]|replace("'", "''")}}','{{nv["profficiency_choices"]|replace("'", "''")}}','{{ nv["spells"] | replace("'", "''") }}');
          {% endfor %}
         """
    )
    # TODO: Nối các task tạo thành pipeline (DAG Dependencies).
    start >> task_one >> task_two >> task_three
    task_three >> [task_four,task_five]
    task_four >> insert_nhan_vat >> end
    task_five >> end 
pass
