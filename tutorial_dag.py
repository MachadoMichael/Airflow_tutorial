from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pandas as pd
from requests
from json
from airflow.operators.bash import BashOperator

def captura_conta_dados(): 
    url = "https://datra.cityofnewyork.us/resouce/rc75-m7u3.json"
    response = request.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def e_valida(ti):
    qtd = ti.xcom_pull(task_id = 'captura_conta_dados')
    if(qtd > 1000):
        return 'valida'
    return 'nvalida'

with DAG('tutorial_dag', start_date = datetime(2021,12,1),
        # schedule_interval funciona como chron do linux
         schedule_interval = '30 * * * *',
         # catchup serve para definir se quer que seja gerada dags da data de inicio até o dia atual
            catchup = False 
            ) as dag:

    captura_conta_dados = PythonOperator(
            task_id = 'captura_conta_dados',
            python_callable = captura_conta_dados
            )
    
    e_valida = BranchPythonOperator(
            task_id = 'e_valida',
            python_callable = e_valida
            )


    valido = BashOperator(
            task_id = 'valido',
            bash_command = "echo 'Quantidade OK'"
            )

    nvalido = BashOperator(
            task_id = 'nvalido',
            bash_command = "echo 'Quantidade não OK'"
            )

    captura_conta_dados >> e_valida >> [valido, nvalido]


    # define a task por task_id
    # inicia a DAG com nome da DAG, data de inicio, schedule_interval, catchup (boa prática)
    # uma task é nada mais do que a chamada de um operador passando como parametro o task_id e o comando 
    # uma task python chama uma função com python_callable
    # uma task bash, usa o bash_command para executar um comando
    # EmptyOperator para chamar start / end
    # uma DAG pode ser iniciada com with ou pode ser definida em uma variavel e colocada dentro da execução
    # também pode se usar um decorator para criar uma dag (@dag("o que fazer")) importar de from airflow.decorators import dag
    # chama @dag(parametros) passa os parametros 


