# Importacion de modulos 
import elasticsearch
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import client
from datetime import datetime
from time import sleep

# Informacion sobre la incidencia
fecha_inicio_incidencia = "2022-03-04 23:50:00" # Meter la hora según timezone
fecha_fin_incidencia    = "2022-03-06 00:10:00" # Meter la hora según timezone

# Nombre del Job de Elastic
jobname                 = "sabado_anomalia"

# Datos de la conexion con el servidor de Elastic
ELASTIC_PASSWORD = "9QJVpAsI1dfU33vZpc072VTd"
CLOUD_ID = "SeriestemporalesElastic:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxMmRiODkwZjhhN2U0YTVjOTAwMDViODMyZjhlYWViNCRjMmU4YmQxYjkzZTI0MGY5OTZiYzIyNTFkZGMxMGY4Yg=="

client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
    
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

elif ((fecha_inicio_incidencia_cambio_timestamp > fecha_cambio_hora_1_timestamp) and (fecha_inicio_incidencia_cambio_timestamp < fecha_cambio_hora_2_timestamp)):
  offset_1 = -7200

else (fecha_cambio_hora_2_timestamp < fecha_inicio_incidencia_cambio_timestamp):
  offset_1 = -3600  

fecha_fin_incidencia_cambio_timestamp = (datetime(2022, fecha_fin_incidencia.month, fecha_fin_incidencia.day, fecha_fin_incidencia.hour, fecha_fin_incidencia.minute, fecha_fin_incidencia.second).timestamp())*1000 

if (fecha_fin_incidencia_cambio_timestamp < fecha_cambio_hora_1_timestamp):
    offset_2 = -3600 

elif ((fecha_fin_incidencia_cambio_timestamp > fecha_cambio_hora_1_timestamp) and (fecha_fin_incidencia_cambio_timestamp < fecha_cambio_hora_2_timestamp)):
  offset_2 = -7200

else (fecha_cambio_hora_2_timestamp < fecha_fin_incidencia_cambio_timestamp):
  offset_2 = -3600  
  
  # Transformacion de variables de entrada
# Fechas
fecha_inicio_incidencia = str((int(fecha_inicio_incidencia_timestamp + offset_1*1000)))
fecha_fin_incidencia = str((int(fecha_fin_incidencia_timestamp + offset_2*1000)))

# Nombre del Datafeed
datafeed_id = "datafeed-" + jobname

# Cliente de ML
cliente_ml = client.ml

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
      latest3 = latest2[latest2[:,0]<float(fecha_inicio_incidencia)]
    # Escoger el id de la reversion correcta
    try:
        id_revert = latest3[0,1]
        id_revert = str(int(id_revert))
    except:
        print("No hay disponible snapshot anterior a la fecha de incidencia. El Script terminará sin éxito")
    else:
        # Paso 3. Revertir el Job antes de la incidencia 
        cliente_ml.revert_model_snapshot(job_id=jobname, snapshot_id=id_revert, delete_intervening_results=True)
        sleep(5)
        
        # Paso 4. Abrir el Job 
        cliente_ml.open_job(job_id=jobname)
        sleep(5)
        
        # Paso 5. Avanzamos datafeed hasta la fecha de inicio de incidencia
        cliente_ml.start_datafeed(datafeed_id=datafeed_id, end=fecha_inicio_incidencia)
        state_datafeed = 1
        
        # Una vez completado el paso continuamos al siguiente
        while (state_datafeed == 1):
          sleep(10)
          state_datafd = cliente_ml.get_datafeed_stats(datafeed_id=datafeed_id)
          st = state_datafd['datafeeds']
          state_actual = st[0]['state'] 
          if (state_actual == 'stopped'):
            state_datafeed = 0
            
        # Paso 6. Abrir el Job 
        cliente_ml.open_job(job_id=jobname)
        sleep(5)
        
        # Paso 7. Avanzamos datafeed desde la fecha de fin de incidencia hasta la fecha actual
        cliente_ml.start_datafeed(datafeed_id=datafeed_id, start=str(int(fecha_fin_incidencia)))
        print("Eliminacion de la incidencia con éxito")
else:
    print("No hay snaphsots disponibles. El Script terminará sin éxito")
