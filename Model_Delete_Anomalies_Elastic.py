#!pip install eland
#!pip install elasticsearch

# Importacion de modulos 
import elasticsearch
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import client
from datetime import datetime
from time import sleep
from elasticsearch.client import IndicesClient
import eland as ed
import pandas as pd
from elasticsearch.client import IngestClient

# ------------------------------------------------------------------------------------INPUTS-------------------------------------------------------------------------------------------

# Informacion sobre la incidencia
fecha_inicio_incidencia = "2022-03-11 23:50:00" # Meter la hora segun timezone, no en UTC
fecha_fin_incidencia    = "2022-03-13 00:10:00" # Meter la hora según timezone, no en UTC

# Nombre del Job de Elastic
jobname                 = "prueba_def"
variable_name_predict   = "y_an"

# Nombre del indice sobre el que se realiza el Job
index_data              = 'prueba_def'

# Datos de la conexion con el servidor de Elastic
ELASTIC_PASSWORD = "1wnMu3ewMODBlwcpl5ceMzUM"
CLOUD_ID = "sextodespliegueseriestemporales:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ4YzRlOTAxOGQzMzk0NTcyOWFkYTU4YjMzMjU5NzY2NCQyODBmYTNiMmEyZTI0OTU5OTcxYWE1MGVmNzJlZmMxMA=="

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Nombre del Datafeed
datafeed_id = "datafeed-" + jobname

# Calculo del offset por cambio de hora

# Timestamps fechas incidencia
fecha_inicio_incidencia = datetime.strptime(fecha_inicio_incidencia, '%Y-%m-%d %H:%M:%S')
fecha_inicio_incidencia_timestamp = datetime.timestamp(fecha_inicio_incidencia)*1000

fecha_fin_incidencia = datetime.strptime(fecha_fin_incidencia, '%Y-%m-%d %H:%M:%S')
fecha_fin_incidencia_timestamp = datetime.timestamp(fecha_fin_incidencia)*1000

# Timestamps de las fechas de cambio de hora

# Primer cambio de hora
fecha_cambio_hora_1 = "2022-03-27 02:00:00" 
fecha_cambio_hora_1 = datetime.strptime(fecha_cambio_hora_1, '%Y-%m-%d %H:%M:%S')
fecha_cambio_hora_1_timestamp = datetime.timestamp(fecha_cambio_hora_1)*1000

# Segundo cambio de hora
fecha_cambio_hora_2 = "2022-10-30 02:00:00" 
fecha_cambio_hora_2 = datetime.strptime(fecha_cambio_hora_2, '%Y-%m-%d %H:%M:%S')
fecha_cambio_hora_2_timestamp = datetime.timestamp(fecha_cambio_hora_2)*1000

# Comparamos fechas en el mismo año 
fecha_inicio_incidencia_cambio_timestamp = (datetime(2022, fecha_inicio_incidencia.month, fecha_inicio_incidencia.day, fecha_inicio_incidencia.hour, fecha_inicio_incidencia.minute, fecha_inicio_incidencia.second).timestamp())*1000 

if (fecha_inicio_incidencia_cambio_timestamp < fecha_cambio_hora_1_timestamp):
    offset_1 = -3600 

if ((fecha_inicio_incidencia_cambio_timestamp > fecha_cambio_hora_1_timestamp) and (fecha_inicio_incidencia_cambio_timestamp < fecha_cambio_hora_2_timestamp)):
  offset_1 = -7200

if (fecha_cambio_hora_2_timestamp < fecha_inicio_incidencia_cambio_timestamp):
  offset_1 = -3600  

fecha_fin_incidencia_cambio_timestamp = (datetime(2022, fecha_fin_incidencia.month, fecha_fin_incidencia.day, fecha_fin_incidencia.hour, fecha_fin_incidencia.minute, fecha_fin_incidencia.second).timestamp())*1000 

if (fecha_fin_incidencia_cambio_timestamp < fecha_cambio_hora_1_timestamp):
    offset_2 = -3600 

if ((fecha_fin_incidencia_cambio_timestamp > fecha_cambio_hora_1_timestamp) and (fecha_fin_incidencia_cambio_timestamp < fecha_cambio_hora_2_timestamp)):
  offset_2 = -7200

if (fecha_cambio_hora_2_timestamp < fecha_fin_incidencia_cambio_timestamp):
  offset_2 = -3600  

# Transformacion de variables de entrada
# Fechas
fecha_inicio_incidencia = (fecha_inicio_incidencia_timestamp + offset_1*1000)/1000
fecha_inicio_incidencia_snapshot = fecha_inicio_incidencia*1000
fecha_fin_incidencia    = (fecha_fin_incidencia_timestamp + offset_2*1000)/1000
fecha_fin_incidencia_snapshot = fecha_fin_incidencia*1000

# Avanzamos esta ultima fecha 2 hora 
fecha_fin_incidencia_snapshot_avanc = str((fecha_fin_incidencia+7200)*1000)

fecha_inicio_incidencia_datetime = datetime.fromtimestamp(fecha_inicio_incidencia)
fecha_fin_incidencia_datetime = datetime.fromtimestamp(fecha_fin_incidencia)

fecha_inicio_incidencia_datetime_str = str(fecha_inicio_incidencia_datetime)
fecha_fin_incidencia_datetime_str = str(fecha_fin_incidencia_datetime)
fecha_inicio_incidencia_datetime_querie = fecha_inicio_incidencia_datetime_str[0:10] + 'T' + fecha_inicio_incidencia_datetime_str[11:]
fecha_fin_incidencia_datetime_querie    = fecha_fin_incidencia_datetime_str[0:10] + 'T' + fecha_fin_incidencia_datetime_str[11:] 

# Nombre del Datafeed
datafeed_id = "datafeed-" + jobname

# Conexion con el servidor de Elastic
es_client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)
cliente_index = IndicesClient(client=es_client)
cliente_ml = es_client.ml

# Creacion del Indice de Backup
indexname = 'indicebackup'
indexname_2 = 'indicebackup2'
indexname_3 = "indicebackup3"

try:
  cliente_index.create(index=indexname)
except:
  cliente_index.delete(index=indexname)
  cliente_index.create(index=indexname)

try:
  cliente_index.create(index=indexname_2)
except:
  cliente_index.delete(index=indexname_2)
  cliente_index.create(index=indexname_2)

try:
  cliente_index.create(index=indexname_3)
except:
  cliente_index.delete(index=indexname_3)
  cliente_index.create(index=indexname_3)

# Reindexar datos de la anomalia al indice backup 
es_client.reindex(body={
      "source": {
          "index": index_data,
          "query": {
              "range":{
                  "@timestamp":{
                      "gte": fecha_inicio_incidencia_datetime_querie,
                      "lte": fecha_fin_incidencia_datetime_querie
                      }
                   }
                }
              },
              "dest": {
                  "index": indexname
              }})

# Obtener datos de la prediccion del Job para el tiempo de anomalia
df = ed.DataFrame(es_client, es_index_pattern=".ml-anomalies-shared")
# Filtramos para nuestro job de interes
df = df[df["job_id"] == jobname]
# Obtenemos las columnas de interes del dataframe
df_select = df[["timestamp", "model_median", "model_upper"]]
# Eliminar filas con valores nulos
df_select = ed.eland_to_pandas(df_select)
df_select = df_select.dropna()
df_select.head(20)
# Ordenamos filas por timestamp
df_select.timestamp
# Seleccionamos timestamp de interes
df_select['timestamp'] = pd.to_datetime(df_select.timestamp)
df_select_order = df_select.sort_values(by="timestamp", ascending=True)
df_select_order = df_select_order.set_index("timestamp")
df_select_filter = df_select_order.loc[fecha_inicio_incidencia_datetime:fecha_fin_incidencia_datetime]

# Hacer un rename del nombre de columna "model_median" al nombre de la variable que se predice en el JOB
df_select_filter.rename(columns = {'model_median':variable_name_predict}, inplace = True)

# Eliminar variable upper_name
df_select_filter = df_select_filter.drop('model_upper', axis=1)
df_select_filter = df_select_filter.reset_index()

# Crear la pipeline de ingesta para @timestamp 
client_Ingest = IngestClient(client=es_client)

try:
  client_Ingest.put_pipeline(id='procesador_date_anomalias', body={
      'description': "Parsea el @timestamp",
      'processors': [
          {
              "date": {
                  "field": "timestamp", 
                  "formats": ["ISO8601"],
                  "target_field": "@timestamp"
                  }
          }
          ]})
except:
  pass

# Introducir datos a un indice intermedio
df_select_filter
for i, row in df_select_filter.iterrows():
    doc = {
        "timestamp": row["timestamp"],
        "y_an": row["y_an"]
    }
    es_client.index(index=indexname_2, id=i, document=doc)

# Eliminar del indice principal datos para el timestamp de la incidencia
es_client.delete_by_query(index=index_data, body={
  "query": {
    "range":{
         "@timestamp":{
            "gte": fecha_inicio_incidencia_datetime_querie,
            "lte": fecha_fin_incidencia_datetime_querie
          }
        }
  }
})
sleep(5)
# Parsear a un indice los datos teniendo cuidado con el timestamp al indice principal
es_client.reindex(body={
      'source': {
          "index": indexname_2
          },
          'dest': {
              "index": index_data,
              "pipeline": "procesador_date_anomalias"
              }},
              wait_for_completion=True)
sleep(5)
# Parsear a un indice los datos teniendo cuidado con el timestamp al indice principal
es_client.reindex(body={
      'source': {
          "index": indexname_2
          },
          'dest': {
              "index": index_data,
              "pipeline": "procesador_date_anomalias"
              }},
              wait_for_completion=True)

# Resetear Job desde la snapshot (buscar snapshot como en el modelo anterior y correr job)
# Paso 1. Parar y cerrar el Job
cliente_ml.close_job(job_id=jobname)
# Paso 2. Escoger el id de la snapshot con latest_record_time_stamp mas cercano a la incidencia
snapshots_available = cliente_ml.get_model_snapshots(job_id=jobname)
# Recoger el count 
number_snapshots = snapshots_available["count"]

if (number_snapshots > 0):
  # Iterar guardando en una matriz el id del snpashot y latest_record_time_stamp
  snapshots_info = snapshots_available["model_snapshots"]
  latest = np.zeros((number_snapshots, 2))
  for i in range(number_snapshots):
    latest[i,0] = snapshots_info[i]["latest_record_time_stamp"]

  for i in range(number_snapshots):
    latest[i,1] = snapshots_info[i]["snapshot_id"]

  # Comparar con la fecha de la incidencia para buscar cual esta inmediatamente antes
    # Ordenar por timestamp (primera columna)
    latest1 = latest[latest[:,0].argsort()]
    latest2 = np.flip(latest1, axis=0)
    # Eliminar valores mayores
    latest3 = latest2[latest2[:,0]<float(fecha_inicio_incidencia_snapshot)]
  # Escoger el id de la reversion correcta
  try:
    id_revert = latest3[0,1]
    id_revert = str(int(id_revert))
  except:
    print("No hay disponible snapshot anterior a la fecha de incidencia. El Script terminará sin éxito")
  else:
    # Paso 3. Revertir el Job antes de la incidencia 
    cliente_ml.revert_model_snapshot(job_id=jobname, snapshot_id=id_revert, delete_intervening_results=True)
    # Paso 4. Abrir el Job 
    cliente_ml.open_job(job_id=jobname)
    # Paso 5. Avanzamos datafeed hasta la fecha de inicio de incidencia
    cliente_ml.start_datafeed(datafeed_id=datafeed_id, end=str(fecha_fin_incidencia_snapshot))
    state_datafeed = 1

    # Una vez completado el paso continuamos al siguiente
    while (state_datafeed == 1):
      sleep(10)
      state_datafd = cliente_ml.get_datafeed_stats(datafeed_id=datafeed_id)
      st = state_datafd['datafeeds']
      state_actual = st[0]['state'] 
      if (state_actual == 'stopped'):
        state_datafeed = 0
    print("Eliminacion de la incidencia con éxito")
else:
    print("No hay snaphsots disponibles. El Script terminará sin éxito")

# Sustituir datos del indice backup al indice original para el tiempo de incidencia
# 1. Eliminar datos ingestados de la prediccion para el tiempo de incidencia
es_client.delete_by_query(index=index_data, body={
  "query": {
    "range":{
         "@timestamp":{
            "gte": fecha_inicio_incidencia_datetime_querie,
            "lte": fecha_fin_incidencia_datetime_querie
          }
        }
  }
})
sleep(5)
# 2. Ingesta de los datos del backup para el tiempo de incidencia
es_client.reindex(body={
      "source": {
          "index": indexname,
          "query": {
              "range":{
                  "@timestamp":{
                      "gte": fecha_inicio_incidencia_datetime_querie,
                      "lte": fecha_fin_incidencia_datetime_querie
                      }
                   }
                }
              },
              "dest": {
                  "index": index_data
              }},
              wait_for_completion=True)
sleep(5)

# Eliminar indices creados
try:
  cliente_index.delete(index=indexname)
except:
  pass
try:
  cliente_index.delete(index=indexname_2)
except:
  pass
try:
  cliente_index.delete(index=indexname_3)
except:
  pass

# Paso 6. Avanzamos datafeed hasta el tiempo real
cliente_ml.open_job(job_id=jobname)
sleep(5)
cliente_ml.start_datafeed(datafeed_id=datafeed_id, start=str(fecha_fin_incidencia_snapshot))