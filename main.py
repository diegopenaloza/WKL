import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import networkx as nx
import requests
import json
import time
import streamlit as st

import pm4py
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
## Import the dfg visualization object
from pm4py.visualization.dfg import visualizer as dfg_visualization

import warnings
import pandas as pd
# Desactivar las advertencias de SettingWithCopyWarning
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)


import streamlit

estado=True


# +
def optain_acces():

  url = "https://accounts.zoho.com/oauth/v2/token"

  payload = 'refresh_token=1000.443c97b2a00e89ba610f0c00f276069e.02c1f47b8e4702dea5ec10680d8d864a&client_id=1000.2Z1OM9RS1ENEI42MN4DCCOKELVTL0K&client_secret=9a5e1ccb574616f1e1d402a00574e8f134bb4b4ca3&grant_type=refresh_token'
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': '_zcsr_tmp=7c2e71bc-ac9f-4423-a9e7-16650f62a6e9; b266a5bf57=9f371135a524e2d1f51eb9f41fa26c60; iamcsr=7c2e71bc-ac9f-4423-a9e7-16650f62a6e9'
  }
  response = requests.request("POST", url, headers=headers, data=payload)
  diccion=json.loads(response.text)
  acceso_token=diccion["access_token"]
  return acceso_token

def optain_jobid(acceso_token):
  # Define la URL de la API
  url = "https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/2479548000000010002/data"

  # Define el JSON que necesitas enviar
  json_data = {
      "responseFormat": "json",
      "sqlQuery": "select * from `Status Case Processing Service and date_ Created_and Timers`"
  }

  # Codifica el JSON para incluirlo en la URL
  encoded_json = json.dumps(json_data)

  # Define los headers
  headers = {
      'ZANALYTICS-ORGID': '764555503',
      'Authorization': f'Zoho-oauthtoken {acceso_token}'
  }

  # Realiza la solicitud GET
  response = requests.get(url, params={"CONFIG": encoded_json}, headers=headers)

  # Verifica si la solicitud fue exitosa
  if response.status_code == 200:
      # Procesa la respuesta
      print("Respuesta:", response.json())
      jobid=response.json()["data"]["jobId"]
      jobid
      return jobid
  else:
      print("Error en la solicitud:", response.status_code)
def optain_table(acceso_token,jobid):

  # Define la URL de la API
  url = f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/2479548000000010002/exportjobs/{jobid}/data"

  # Define los headers
  headers = {
      'ZANALYTICS-ORGID': '764555503',
      'Authorization': f'Zoho-oauthtoken {acceso_token}'
  }

  # Realiza la solicitud GET
  response = requests.get(url, headers=headers)

  # Verifica si la solicitud fue exitosa
  if response.status_code == 200:
      # Procesa la respuesta
      print("Se optuvo la data")
  else:
      print("Error en la solicitud:", response.status_code)

  diccion=json.loads(response.text)

  copy_calls=pd.DataFrame(diccion["data"])

  return copy_calls


# -

if estado:
    acceso_token=optain_acces()
    jobid=optain_jobid(acceso_token)

    try:
        df_call = optain_table(acceso_token, jobid)
    except Exception as e:
        print(f"Error: {e}")
        print("Esperando 20 segundos antes de volver a intentar...")
        time.sleep(20)

        # Intenta nuevamente después de esperar 20 segundos
        try:
            df_call = optain_table(acceso_token, jobid)
        except Exception as e:
            print(f"Error en el segundo intento: {e}")
            
    df_data=df_call.copy()
    # Convertir la columna 'time:timestamp' a formato de fecha y hora
    df_data['time:timestamp'] = pd.to_datetime(df_data['time:timestamp'], format='%d/%m/%Y %I:%M %p')
    df_data['Date_Created_Case_Processing'] = pd.to_datetime(df_data['Date_Created_Case_Processing'], format='%d %b, %Y %H:%M:%S')
    df_logs=df_data.copy()

# +
# df_call=optain_table(acceso_token,jobid)
# -



def days(transitions_matrix,status,df):
  column_names = ['IN PREPARATION FIRST STAGE (1)','IN PREPARATION SECOND STAGE (1)','CLIENT APPROVAL PENDING (1)','SECOND QA BY MANAGEMENT (1)','UNDER REVIEW - NO PAYMENT (1)','UNDER REVIEW - NO PAYMENT (1)','No value (1)','DECISION RECEIVED (1)','PROCESSING BY CANADIAN ENTITY / CONSULTANCY DELIVERED (1)',"ON HOLD (1)", "NEW (1)",'SLOW PROGRESS - PENDING INFORMATION OR DOCUMENTS (2 MONTHS) (1)','UNDER REVIEW (1)','IN QUEUE (1)','PROCESSING BY CANADIAN ENTITY (1)']

  for column_name in column_names:
      if column_name not in transitions_matrix.columns:
          transitions_matrix[column_name] = 0



  # Crear un grafo dirigido desde la matriz de transición
  G = nx.from_pandas_adjacency(transitions_matrix, create_using=nx.DiGraph)

  # Definir la función para obtener el siguiente estado basado en las probabilidades de transición más altas
  def siguiente_estado_probable(actual):
      if actual not in G or not G[actual]:
          return None
      siguientes = list(G.successors(actual))
      if not siguientes:
          return None
      prob_max = max(G[actual][siguiente]['weight'] for siguiente in siguientes)
      siguiente_probable = [siguiente for siguiente in siguientes if G[actual][siguiente]['weight'] == prob_max][0]
      return siguiente_probable

  # Ejemplo de uso para obtener la secuencia más probable hasta "Completed"
  estado_actual = status
  secuencia_estados = [estado_actual]
  estados_visitados = set()
  duracion_promedio_por_transicion = {}  # Nuevo diccionario para almacenar los promedios de duración por transición
  total_h_promedio_por_transicion = {}  # Nuevo diccionario para almacenar los promedios de total_h_by_status por transición

  while True:
      siguiente = siguiente_estado_probable(estado_actual)
      if siguiente:
          if siguiente in estados_visitados:
              # print("Se detectó un bucle. Terminando el proceso.")
              break

          # Filtrar el DataFrame para la transición actual
          transicion_actual = df[(df['concept:name'] == estado_actual) & (df['next_state'] == siguiente)]
          # Asegúrate de que la columna "duration" y "total_h_by_status" sean de tipo numérico
          transicion_actual["duration"] = pd.to_numeric(transicion_actual["duration"], errors='coerce')
          transicion_actual["tota_h_by_status"] = pd.to_numeric(transicion_actual["tota_h_by_status"], errors='coerce')

          # Calcular el promedio de duración para la transición actual y almacenarlo en el diccionario
          duracion_promedio = transicion_actual["duration"].mean()
          duracion_promedio_por_transicion[(estado_actual, siguiente)] = duracion_promedio

          # Calcular el promedio de total_h_by_status para la transición actual y almacenarlo en el diccionario
          total_h_promedio = transicion_actual["tota_h_by_status"].mean()
          total_h_promedio_por_transicion[(estado_actual, siguiente)] = total_h_promedio

          secuencia_estados.append(siguiente)
          estados_visitados.add(siguiente)
          estado_actual = siguiente
      else:
          break

  # print("EL Camino mas probable:", " -> ".join(secuencia_estados))

  # # Imprimir los promedios de duración por transición
  # for transicion, promedio in duracion_promedio_por_transicion.items():
  #     print(f"Duración promedio para la transición {transicion}: {promedio}")

  # # Imprimir los promedios de total_h_by_status por transición
  # for transicion, promedio in total_h_promedio_por_transicion.items():
  #     print(f"Total_h promedio para la transición {transicion}: {promedio}")

  # Extraer solo los valores numéricos
  valores_numericos = list(duracion_promedio_por_transicion.values())
  valores_numericos_horas_trabajadas = list(total_h_promedio_por_transicion.values())
  # Imprimir la lista de valores numéricos
  # print(sum(valores_numericos))
  # print(secuencia_estados)
  # print(list(duracion_promedio_por_transicion.values()))
  # print(list(total_h_promedio_por_transicion.values()))


  # while True:
  #     siguiente = siguiente_estado_probable(estado_actual)
  #     if siguiente:
  #         if siguiente in estados_visitados:
  #             print("Se detectó un bucle. Terminando el proceso.")
  #             break

  #         # Filtrar el DataFrame para la transición actual
  #         transicion_actual = df[(df['concept:name'] == estado_actual) & (df['next_state'] == siguiente)]
  #         # Asegúrate de que la columna "duration" sea de tipo numérico
  #         transicion_actual["duration"] = pd.to_numeric(transicion_actual["duration"], errors='coerce')

  #         # Calcular el promedio de duración para la transición actual y almacenarlo en el diccionario
  #         duracion_promedio = transicion_actual["duration"].mean()
  #         duracion_promedio_por_transicion[(estado_actual, siguiente)] = duracion_promedio

  #         secuencia_estados.append(siguiente)
  #         estados_visitados.add(siguiente)
  #         estado_actual = siguiente
  #     else:
  #         break

  # print("EL Camino mas probable:", " -> ".join(secuencia_estados))

  # # Imprimir los promedios de duración por transición
  # for transicion, promedio in duracion_promedio_por_transicion.items():
  #     print(f"> {transicion}: {promedio}")

  # # Extraer solo los valores numéricos
  # valores_numericos = list(duracion_promedio_por_transicion.values())
  # # Imprimir la lista de valores numéricos


    # Datos proporcionados
  estados = secuencia_estados[:-1]

  tiempo_total =valores_numericos

  tiempo_trabajo = valores_numericos_horas_trabajadas

  # Crear un DataFrame
  df = pd.DataFrame({'Estado': estados, 'Tiempo Total': tiempo_total, 'Tiempo Trabajo': tiempo_trabajo})
  df["Trabajo por dia"]=df["Tiempo Trabajo"]/df["Tiempo Total"]

  # display(df)

  return  secuencia_estados , valores_numericos,valores_numericos_horas_trabajadas, df


def carga(df_logs,servicio, level,complexity,status='NEW (1)',id_case=''):



  df_logs_select=df_logs[["case:concept:name","time:timestamp","concept:name","Service Name","Date_Created_Case_Processing","duration","CS_Level","Case_Complexity","tota_h_by_status"]]

  # select_case=df_logs_select[df_logs_select["case:concept:name"]==id_case].reset_index(drop=True)
  # display(select_case)

  df_logs_select=df_logs_select[df_logs_select["Service Name"].str.contains(servicio)]



  # df_logs_select=df_logs_select[df_logs_select["CS_Level"]==level]
  # df_logs_select=df_logs_select[df_logs_select["Case_Complexity"]==complexity]




  df_logs_select = df_logs_select.sort_values(by=[ 'case:concept:name','time:timestamp'])

  # df_logs_select = df_logs_select[df_logs_select['Date_Created_Case_Processing'] >= '2023-01-01']




  ###

  # # Filtrar los casos que tienen 'NEW' en 'concept:name' y han comenzado en 2023
  # filtered_cases = df_logs_select[(df_logs_select['concept:name'] == 'NEW') & (df_logs_select['time:timestamp'].dt.year == 2023)]['case:concept:name'].unique()
  # # Filtrar el DataFrame original para mantener todos los registros de esos 'case:concept:name'
  # df_logs_select_filter = df_logs_select[df_logs_select['case:concept:name'].isin(filtered_cases)]


  ###



  df_logs_select_filter=df_logs_select



  df=df_logs_select_filter.copy()
  # Asegúrate de que la columna "duration" sea de tipo numérico
  df["duration"] = pd.to_numeric(df["duration"], errors='coerce')

  # Ordenar el DataFrame por caso y tiempo
  df.sort_values(by=['case:concept:name', 'time:timestamp'], inplace=True)



    # Función para agregar números a los estados repetidos reiniciando para cada case:concept:name
  def add_count(row):
      global counts
      case_name = row['case:concept:name']
      state = row['concept:name']

      if case_name not in counts:
          counts[case_name] = {}

      if state in counts[case_name]:
          counts[case_name][state] += 1
      else:
          counts[case_name][state] = 1

      return f"{state} ({counts[case_name][state]})"

  # Aplicar la función a cada fila del DataFrame
  df['concept:name'] = df.apply(add_count, axis=1)

  # Ordenar el DataFrame por 'case:concept:name' y 'time:timestamp' para asegurar un orden adecuado
  df = df.sort_values(by=['case:concept:name', 'time:timestamp'])
  # Crear una columna 'next_state' representando el estado siguiente
  df['next_state'] = df.groupby('case:concept:name')['concept:name'].shift(-1)





    #Obtener los índices de las filas con la última fecha para cada "Id_Case_Processing"
  idx_last_date = df.groupby('case:concept:name')['time:timestamp'].idxmax()

  # Filtrar el DataFrame original usando los índices obtenidos
  result_df = df.loc[idx_last_date]


  # Calcular la diferencia en días
  today = datetime.now()
  result_df['duration'] = (today - result_df['time:timestamp']).dt.days


  # Eliminar filas con NaN en la columna 'next_state'
  df = df.dropna(subset=['next_state'])



  # Calcular la matriz de transición de probabilidades
  transitions_matrix = pd.crosstab(df['concept:name'], df['next_state'], normalize='index')

  result=days(transitions_matrix,status,df)
  ##############
  #########################
  # print(id_case)
  ###############################
  ##################

  select_case=result_df[result_df["case:concept:name"]==id_case].reset_index(drop=True)
  # display(select_case)
  # Restar 15 a la fila donde el valor de Estado es "IN PREPARATION FIRST STAGE (1)" en la columna "Tiempo Total"
  df_old=result[3]
  # print('no hay')
  # print('no hay', select_case["duration"][0])
  df_old.loc[df_old['Estado'] == select_case["concept:name"][0], 'Tiempo Total'] -= select_case["duration"][0]
  df_old["id_case"]=id_case
  # display('Old',df_old)



    # Rellenar NaN en Tiempo Trabajo con 0
  df_old['Tiempo Trabajo'].fillna(0, inplace=True)
  # display('Old No nana',df_old)
  # Crear una nueva lista de filas replicadas según el tiempo total
  new_rows = []
  fecha = datetime.now()
  for index, row in df_old.iterrows():
      tiempo_total = row['Tiempo Total']
      if not pd.isna(tiempo_total):
          for i in range(int(tiempo_total)):
              new_row = {'Fecha': fecha, 'Estado': row['Estado'], 'Trabajo por dia': row['Trabajo por dia'], 'Id Case': row['id_case']}
              new_rows.append(new_row)

              fecha += timedelta(days=1)

  # Crear DataFrame con las nuevas filas
  df_resultado = pd.DataFrame(new_rows)
  # df_resultado = df_resultado.dropna(subset=['Trabajo por dia'])
  try:
    df_resultado = df_resultado.dropna(subset=['Trabajo por dia'])
      # Mostrar DataFrame resultante
    # display(df_resultado)
  except:
    # print('No agrega  Trabajo')
    pass


  return result, df,transitions_matrix, result_df,df_old, df_resultado

if estado:
    df_logs=df_data.copy()
    df_logs_pass=df_data.copy()
    # Inicializar el diccionario de conteo
    counts = {}
    # Función para agregar números a los estados repetidos reiniciando para cada case:concept:name
    def add_count(row):
        global counts
        case_name = row['case:concept:name']
        state = row['concept:name']

        if case_name not in counts:
            counts[case_name] = {}

        if state in counts[case_name]:
            counts[case_name][state] += 1
        else:
            counts[case_name][state] = 1

        return f"{state} ({counts[case_name][state]})"

    # Aplicar la función a cada fila del DataFrame
    df_logs['concept:name'] = df_logs.apply(add_count, axis=1)

    # Ordenar el DataFrame por 'case:concept:name' y 'time:timestamp' para asegurar un orden adecuado
    df_logs = df_logs.sort_values(by=['case:concept:name', 'time:timestamp'])
    # Crear una columna 'next_state' representando el estado siguiente
    df_logs['next_state'] = df_logs.groupby('case:concept:name')['concept:name'].shift(-1)
    df_logs=df_logs[["case:concept:name","time:timestamp","concept:name","Service Name","Date_Created_Case_Processing","duration","CS_Level","Case_Complexity","tota_h_by_status",'next_state',"LMIA or IMM Case Owner","LMIA or IMM Case Owner Name"]]
     #Obtener los índices de las filas con la última fecha para cada "Id_Case_Processing"
    idx_last_date = df_logs.groupby('case:concept:name')['time:timestamp'].idxmax()
    # Filtrar el DataFrame original usando los índices obtenidos
    result_df = df_logs.loc[idx_last_date]


if estado:
    CS_List=df_logs[["LMIA or IMM Case Owner","LMIA or IMM Case Owner Name"]].drop_duplicates()["LMIA or IMM Case Owner"]
    CS_List_name=df_logs[["LMIA or IMM Case Owner","LMIA or IMM Case Owner Name"]].drop_duplicates()["LMIA or IMM Case Owner Name"]
    lista_CS_selec=[]
    lista_carga=[]
    for k,j in zip(CS_List,CS_List_name):
      cases_by_owner=result_df[result_df["LMIA or IMM Case Owner"]==k]
      dfs_select=[]
      for i in cases_by_owner["case:concept:name"]:
        # print(i)
        id_case_select=i
        selecionado=result_df[result_df["case:concept:name"]==id_case_select].reset_index(drop=True)
        servicio_as_case=selecionado["Service Name"][0]
        CS_Level=selecionado["CS_Level"][0]
        Case_Complexity=selecionado["Case_Complexity"][0]
        concept_name=selecionado["concept:name"][0]
        counts = {}
        transitions_matrix=carga(df_logs=df_logs_pass,servicio=servicio_as_case[0:5],level=CS_Level,complexity=Case_Complexity,status=concept_name,id_case=id_case_select)
        dfs_select.append(transitions_matrix[5])


      # Unir los dataframes verticalmente
      df_concatenado = pd.concat(dfs_select, ignore_index=True)
      # Ordenar por la columna 'Fecha'
      df_concatenado = df_concatenado.sort_values(by='Fecha')
      fecha_hoy = datetime.now().date()
      # Selecciona las filas con la fecha de hoy
      df_hoy = df_concatenado[df_concatenado['Fecha'].dt.date == fecha_hoy]
      # Muestra la información del nuevo DataFrame
      # df_hoy.info()
      print("Trabajo del Dia :  ","El CS es",k,"Nombre: ",j, "Su carga de Trabajo es: ", df_hoy["Trabajo por dia"].sum())
      lista_CS_selec.append(k)
      lista_carga.append(df_hoy["Trabajo por dia"].sum())
        
    wkl=pd.DataFrame({"code": lista_CS_selec, "Horas": lista_carga}).sort_values(by="Horas", ascending=False)
    # Convertir a JSON
    json_data = wkl.to_json(orient="records")
    # Puedes imprimir para verificar o retornar json_data desde tu ruta de Flask
    csv_variable = wkl.to_csv(index=False)
    st.text(csv_variable)

