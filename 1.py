import random
import os

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def hello():
    print("Airflow")

def task_a():
    print(random.uniform(0,100))
    print(random.uniform(0,1000))

def task_c():
    with open('/opt/airflow/dags/file_task_c.txt', 'r') as file:
        data = file.readlines()

    data[-1:] = str(random.uniform(0,100)) + " " + str(random.uniform(0,100))+ "\n"
    with open('/opt/airflow/dags/file_task_c.txt', 'w') as file:
        file.writelines(data)
    #file.writelines(str(random.uniform(0,100)) + " " + str(random.uniform(0,100))+ "\n")[-1:]
    #file.close()

def task_d():
    a = 0.
    b = 0.
    with open('/opt/airflow/dags/file_task_c.txt', "r") as file:
        line = file.readline()
        while line:
            print(line, end="")            
            a = a + float(line.split(' ', 2)[0])
            print(a)
            b = b + float(line.split(' ', 2)[1])
            print(b)
            line = file.readline()
    file.close()
    file = open('/opt/airflow/dags/file_task_c.txt', 'a')
    file.writelines(str(a-b))
    file.close()

with DAG(dag_id="first_dag", start_date=datetime(2022, 12, 13), schedule="40-45 2 * * *", catchup=False, max_active_runs=1) as dag:
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task = PythonOperator(task_id="world", python_callable = hello)
    
    #a.      Создайте еще один PythonOperator, который генерирует два произвольных числа и печатает их. Добавьте вызов нового оператора в конец вашего pipeline с помощью >>.
    task_a = PythonOperator(task_id="task_a", python_callable = task_a)

    #b.      Попробуйте снова запустить ваш DAG и убедитесь, что все работает верно.

    #c.      Если запуск произошел успешно, попробуйте изменить логику вашего Python-оператора следующим образом – сгенерированные числа должны записываться в текстовый файл – через пробел. 
    # При этом должно соблюдаться условие, что каждые новые два числа должны записываться с новой строки не затирая предыдущие.
    task_c = PythonOperator(task_id="task_c", python_callable = task_c)

    #d.      Создайте новый оператор, который подключается к файлу и вычисляет сумму всех чисел из первой колонки, 
    # затем сумму всех чисел из второй колонки и рассчитывает разность полученных сумм. 
    # Вычисленную разность необходимо записать в конец того же файла, не затирая его содержимого.
    task_d = PythonOperator(task_id="task_d", python_callable = task_d)

    #e.      Измените еще раз логику вашего оператора из пунктов 12.а – 12.с.
    #  При каждой новой записи произвольных чисел в конец файла, вычисленная сумма на шаге 12.d должна затираться.

    #f.       Настройте ваш DAG таким образом, чтобы он запускался каждую минуту в течение 5 минут.

    
    #bash_task >> python_task >> task_a >> task_c >> task_d
    bash_task >> python_task >> task_c >> task_d

    