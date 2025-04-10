import pandas as pd
import os
import uuid
from fastapi import FastAPI, File, UploadFile, HTTPException

app = FastAPI()


def procesar_excel(input_file):
    try:
        
        # Leer archivo Excel
        archivo = pd.read_excel(input_file, engine='xlrd', header=1)
        
        # Eliminar la fila que contiene "Asistencia Diaria"
        archivo = archivo[~archivo.iloc[:, 0].astype(str).str.contains('Asistencia Diaria', na=False)]
        
        # Eliminar columnas especificadas
        archivo = archivo.drop(columns=['Hora mas temprana', 'última Hora'], errors='ignore')
        
        # Obtener los IDs únicos de los usuarios
        ids_usuarios = archivo['ID de Usuario'].unique()
        
        for id_usuario in ids_usuarios:
            registros_usuario = archivo[archivo['ID de Usuario'] == id_usuario]
                
            for i in range(len(registros_usuario) - 1):
                index_actual = registros_usuario.index[i]
                index_siguiente = registros_usuario.index[i + 1]
                
                row_actual = archivo.loc[index_actual]
                row_siguiente = archivo.loc[index_siguiente]
                
                horas_registro_actual = row_actual['Hora de Registro'].split(';')
                horas_registro_actual = [pd.to_datetime(hora, format='%H:%M:%S', errors='coerce') for hora in horas_registro_actual]
                
                primera_hora_actual = horas_registro_actual[0].hour if horas_registro_actual[0] is not pd.NaT else None
                
                horas_registro_siguiente = row_siguiente['Hora de Registro'].split(';')
                horas_registro_siguiente = [pd.to_datetime(hora, format='%H:%M:%S', errors='coerce') for hora in horas_registro_siguiente]
                
                primera_hora_siguiente = horas_registro_siguiente[0].hour if horas_registro_siguiente[0] is not pd.NaT else None
                
                if row_actual['registrar los tiempos'] == 1 and (primera_hora_actual is not None and primera_hora_actual >= 19):
                    if len(horas_registro_siguiente) == 1 and primera_hora_siguiente is not None and primera_hora_siguiente < 7:
                        nueva_hora_registro = [str(hora.time()) for hora in horas_registro_actual] + [str(horas_registro_siguiente[0].time())]
                        archivo.at[index_siguiente, 'Hora de Registro'] = ''
                    else:
                        horas_siguiente_dia = horas_registro_siguiente[:-1]
                        nueva_hora_registro = [str(hora.time()) for hora in horas_registro_actual] + [str(hora.time()) for hora in horas_siguiente_dia]
                        horas_restantes_siguiente_dia = row_siguiente['Hora de Registro'].split(';')[-1:]
                        archivo.at[index_siguiente, 'Hora de Registro'] = ';'.join(horas_restantes_siguiente_dia)
                    
                    archivo.at[index_actual, 'Hora de Registro'] = ';'.join(nueva_hora_registro)
                    archivo.at[index_actual, 'registrar los tiempos'] = len(nueva_hora_registro)
                    archivo.at[index_siguiente, 'registrar los tiempos'] = len(horas_restantes_siguiente_dia)
                
                
                # Mover todas las horas del siguiente día al final del penúltimo turno
                if i == len(registros_usuario) - 2 and primera_hora_actual > 20:
                    if len(horas_registro_siguiente) > 0:
                        nueva_hora_registro = [str(hora.time()) for hora in horas_registro_actual] + [str(hora.time()) for hora in horas_registro_siguiente]
                        archivo.at[index_actual, 'Hora de Registro'] = ';'.join(nueva_hora_registro)
                        archivo.at[index_siguiente, 'Hora de Registro'] = ''
                        archivo.at[index_siguiente, 'registrar los tiempos'] = 0
        
        # Crear la columna 'Entry'
        def determinar_entry(horas):
            if not horas:
                return 'N/A'
            horas_list = [pd.to_datetime(hora, format='%H:%M:%S', errors='coerce') for hora in horas.split(';')]
            for i in range(len(horas_list) - 1):
                if horas_list[i].hour == horas_list[i + 1].hour and horas_list[i].minute == horas_list[i + 1].minute and (horas_list[i + 1] - horas_list[i]).seconds > 30:
                    
                    return horas_list[i].strftime('%H:%M:%S')
            return horas_list[0].strftime('%H:%M:%S')

        archivo['Entry'] = archivo['Hora de Registro'].apply(lambda x: determinar_entry(x) if x else 'N/A')

        # Crear la columna 'Exit'
        def determinar_exit(horas):
            if not horas:
                return 'N/A'
            horas_list = horas.split(';')
            if horas_list[-1] != determinar_entry(horas):
                ultima_hora = horas_list[-1]    
            else:
                ultima_hora = 'N/A'
            return ultima_hora

        archivo['Exit'] = archivo['Hora de Registro'].apply(lambda x: determinar_exit(x) if x else 'N/A')

        # Crear la columna 'Turno'
        def determinar_turno(hora, salida, fecha):
            fecha_dt = pd.to_datetime(fecha, format='%Y-%m-%d', errors='coerce')
            es_miercoles = fecha_dt.dayofweek == 2
            es_viernes = fecha_dt.dayofweek == 4

            if not hora:
                return 'N/A'
            hora_dt = pd.to_datetime(hora, format='%H:%M:%S', errors='coerce')
            salida_dt = pd.to_datetime(salida, format='%H:%M:%S', errors='coerce') if salida else None
            if hora_dt.hour >= 20 or hora_dt.hour < 3:
                return 3
            elif (hora_dt.hour == 5 and hora_dt.minute >= 0) or (hora_dt.hour == 6 and hora_dt.minute <= 20 and salida_dt.hour <= 15):
                return 1
            elif (((hora_dt.hour == 6 and hora_dt.minute >= 23) or (hora_dt.hour == 7 and hora_dt.minute <= 45)) and ((salida_dt.hour == 15 and salida_dt.minute >= 20 and es_viernes) or salida_dt.hour == 17 or salida_dt.hour == 18 or (salida_dt.hour == 16 and salida_dt.minute <= 30 and es_miercoles))):
                return 19
            elif ((hora_dt.hour == 7 or (hora_dt.hour == 6 and hora_dt.minute >= 40) ) and ((salida_dt.hour <= 17 and (salida_dt.hour >= 13 and salida_dt.minute >= 40)) or (salida_dt.hour == 16 and salida_dt.minute <= 30))):
                return 6
            elif (hora_dt.hour == 13 and hora_dt.minute >= 10) or (hora_dt.hour == 14 and hora_dt.minute <= 50):
                return 2
            else:
                return 'consultar'
        archivo['Turno'] = archivo.apply(lambda row: determinar_turno(row['Hora de Registro'].split(';')[0] if row['Hora de Registro'] else 'N/A', row['Exit'], row['Grabar fecha']), axis=1)

        # Crear la columna 'LaunchEntry' y 'LaunchExit'
        def determinar_launch_entry_exit(horas, turno, fecha, entry, exit):
            if not horas or turno == 'N/A':
                return 'N/A', 'N/A'
            horas_list = [pd.to_datetime(hora, format='%H:%M:%S', errors='coerce') for hora in horas.split(';')]

            if turno == 1 :
                ventanas = [
                    (pd.to_datetime('07:50:00', format='%H:%M:%S'), pd.to_datetime('10:10:00', format='%H:%M:%S')),
                    (pd.to_datetime('11:20:00', format='%H:%M:%S'), pd.to_datetime('13:40:00', format='%H:%M:%S'))
                ]
            elif turno == 2:
                ventanas = [
                    (pd.to_datetime('17:20:00', format='%H:%M:%S'), pd.to_datetime('19:40:00', format='%H:%M:%S'))
                ]
            elif turno == 19 or turno == 6:
                ventanas = [
                    (pd.to_datetime('11:20:00', format='%H:%M:%S'), pd.to_datetime('13:40:00', format='%H:%M:%S'))
                ]
            elif turno == 3:
                ventanas = [
                    (pd.to_datetime('00:50:00', format='%H:%M:%S'), pd.to_datetime('02:40:00', format='%H:%M:%S'))
                ]
            else:
                return 'N/A', 'N/A'
            
            launch_entry = 'N/A'
            launch_exit = 'N/A'

            for inicio, fin in ventanas:
                # Determinar LaunchEntry
                
                for i in range(len(horas_list) - 1):
                    if (inicio <= horas_list[i] <= fin and 
                        (horas_list[i + 1] - horas_list[i]).total_seconds() > 30 and 
                        horas_list[i].strftime('%H:%M:%S') != entry and
                        horas_list[i].strftime('%H:%M:%S') != exit):
                            launch_entry = horas_list[i].strftime('%H:%M:%S')
                            break
                
                if(launch_entry != 'N/A'):
                    # Determinar LaunchExit
                    for i in range(len(horas_list) - 1):
                        if (inicio <= horas_list[i] <= fin and 
                            (horas_list[i+1] - horas_list[i]).total_seconds() > 30 and 
                            horas_list[i+1].strftime('%H:%M:%S') != entry and
                            horas_list[i+1].strftime('%H:%M:%S') != exit and
                            (horas_list[i+1] - horas_list[i]).total_seconds() / 60 > 10 and
                            horas_list[i+1].strftime('%H:%M:%S') > launch_entry):
                                launch_exit = horas_list[i+1].strftime('%H:%M:%S')
                                break
                    break

                if(launch_exit != 'N/A'):
                    launch_entry = 'N/A'
                    break
                
            return launch_entry, launch_exit

        archivo[['LaunchEntry', 'LaunchExit']] = archivo.apply(lambda row: pd.Series(determinar_launch_entry_exit(row['Hora de Registro'], row['Turno'], row['Grabar fecha'], row['Entry'], row['Exit'])) if row['Hora de Registro'] else pd.Series(['N/A', 'N/A']), axis=1)

        def determinar_registros_adicionales(horas, entry, exit, launch_entry, launch_exit):
            if not horas:
                return 'N/A'
            horas_list = [pd.to_datetime(hora, format='%H:%M:%S', errors='coerce') for hora in horas.split(';')]
            registros_adicionales = []
            for hora in horas_list:
                if (hora.strftime('%H:%M:%S') != entry and
                    hora.strftime('%H:%M:%S') != exit and
                    hora.strftime('%H:%M:%S') != launch_entry and
                    hora.strftime('%H:%M:%S') != launch_exit):
                    registros_adicionales.append(hora.strftime('%H:%M:%S'))
            return ';'.join(registros_adicionales) if registros_adicionales else 'N/A'

        archivo['Registros_Adicionales'] = archivo.apply(lambda row: determinar_registros_adicionales(row['Hora de Registro'], row['Entry'], row['Exit'], row['LaunchEntry'], row['LaunchExit']) if row['Hora de Registro'] else 'N/A', axis=1)
       
        json_resultado = archivo.to_json(orient='records')
        return json_resultado
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Error: La columna {e} no se encuentra en el DataFrame.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error: {str(e)}")


@app.post("/procesar_excel")
async def procesar_archivo_excel(file: UploadFile = File(...)):
    temp_file_path = f"./{uuid.uuid4()}_{file.filename}"  # Generar nombre único para el archivo
    try:
        if not file.filename.lower().endswith(('.xls', '.xlsx')):
            raise HTTPException(status_code=400, detail="El archivo debe ser un Excel con extensión .xls o .xlsx.")

        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(await file.read())
        
        json_resultado = procesar_excel(temp_file_path)


        return json_resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ocurrio un error: {str(e)}")
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)