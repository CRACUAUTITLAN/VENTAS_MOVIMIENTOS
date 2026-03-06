import streamlit as st
import pandas as pd
import io
import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(page_title="Análisis Global Ventas - CRA", layout="wide")
st.title("🌍 CRA INT: Análisis Global de Ventas y Demanda")
st.markdown("Análisis macro de ventas (2025 - 2026) por Línea y Categoría a nivel compañía.")

# --- CONEXIÓN A GOOGLE DRIVE ---
@st.cache_resource
def get_drive_service():
    try:
        gcp_creds = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            gcp_creds, scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"⚠️ Error de conexión: {e}")
        st.stop()

drive_service = get_drive_service()
MASTER_SALES_ID = st.secrets["general"].get("master_sales_id")
PARENT_FOLDER_ID = st.secrets["general"]["drive_folder_id"]

# --- FUNCIONES DRIVE ---
def descargar_archivo_drive(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        file.seek(0)
        return file
    except Exception: return None

def buscar_archivos_ventas_globales():
    archivos_encontrados = []
    if not MASTER_SALES_ID: return []
    
    sucursales = ["CUAUTITLAN", "TULTITLAN", "BAJIO"]
    anios = ["2025", "2026"]
    
    for suc in sucursales:
        for anio in anios:
            query = f"name contains '{suc}' and name contains '{anio}' and name contains 'MASTER' and '{MASTER_SALES_ID}' in parents and trashed=false"
            results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            archivos_encontrados.extend(results.get('files', []))
            
    return archivos_encontrados

def subir_excel_a_drive(buffer, nombre_archivo):
    try:
        media = MediaIoBaseUpload(buffer, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
        file_metadata = {'name': nombre_archivo, 'parents': [PARENT_FOLDER_ID]}
        archivo = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True).execute()
        return archivo.get('webViewLink')
    except Exception as e: 
        st.error(f"Error subiendo a Drive: {e}")
        return None

# --- MOTOR DE ANÁLISIS DE DATOS ---
def clasificar_demanda(hits):
    if hits > 12: return "ALTA"
    elif hits >= 6: return "MEDIA"
    elif hits > 0: return "BAJA"
    else: return "NULA"

def procesar_analisis_global(bar_obj):
    files_metadata = buscar_archivos_ventas_globales()
    if not files_metadata: return None
    
    dfs = []
    total_files = len(files_metadata)
    
    # 1. Descarga Masiva
    for i, file_meta in enumerate(files_metadata):
        content = descargar_archivo_drive(file_meta['id'])
        if content:
            try:
                engine = 'xlrd' if 'xls' in file_meta['name'] and 'xlsx' not in file_meta['name'] else 'openpyxl'
                df_temp = pd.read_excel(content, engine=engine)
                df_temp.columns = df_temp.columns.str.upper().str.strip()
                
                # Solo traemos las columnas que nos importan para no saturar la memoria
                cols_necesarias = [c for c in df_temp.columns if c in ['NP', 'DESCR', 'CATEGORIA', 'LINEA', 'CANTIDAD']]
                dfs.append(df_temp[cols_necesarias])
            except Exception: pass
        
        # Actualizar barra
        progreso = int((i + 1) / total_files * 40) # Toma el 40% del proceso
        bar_obj.progress(progreso, text=f"Descargando bases de datos ({i+1}/{total_files})...")
        
    if not dfs: return None
    
    bar_obj.progress(45, text="Unificando matriz de datos global...")
    df_global = pd.concat(dfs, ignore_index=True)
    
    # Limpieza de datos base
    df_global['NP'] = df_global['NP'].astype(str).str.strip()
    df_global['CANTIDAD'] = pd.to_numeric(df_global['CANTIDAD'], errors='coerce').fillna(0)
    
    # Si alguna fila no tiene categoría o línea, le ponemos "SIN DEFINIR"
    if 'CATEGORIA' not in df_global.columns: df_global['CATEGORIA'] = "SIN DEFINIR"
    if 'LINEA' not in df_global.columns: df_global['LINEA'] = "SIN DEFINIR"
    
    df_global['CATEGORIA'] = df_global['CATEGORIA'].fillna("SIN DEFINIR").astype(str).str.strip()
    df_global['LINEA'] = df_global['LINEA'].fillna("SIN DEFINIR").astype(str).str.strip()
    
    bar_obj.progress(60, text="Agrupando por Número de Parte y calculando métricas...")
    
    # 2. Agrupación Matemática
    # Agrupamos por NP y tomamos la primera descripción, categoria y linea que aparezca. Y sumamos cantidades.
    resumen = df_global.groupby('NP').agg(
        DESCR=('DESCR', 'first'),
        CATEGORIA=('CATEGORIA', 'first'),
        LINEA=('LINEA', 'first'),
        VENTA=('CANTIDAD', 'sum'),
        total_ev=('CANTIDAD', 'count'),
        neg_ev=('CANTIDAD', lambda x: (x < 0).sum())
    ).reset_index()
    
    bar_obj.progress(80, text="Calculando HITS y nivel de DEMANDA...")
    
    # 3. Cálculo de HITS y DEMANDA
    resumen['HITS'] = (resumen['total_ev'] - (resumen['neg_ev'] * 2)).clip(lower=0)
    
    # Filtramos piezas que no tuvieron ni venta ni hits reales (basura del sistema)
    resumen = resumen[(resumen['VENTA'] != 0) | (resumen['HITS'] > 0)].reset_index(drop=True)
    
    # Aplicar clasificación de demanda
    resumen['DEMANDA'] = resumen['HITS'].apply(clasificar_demanda)
    
    # Seleccionar y ordenar las columnas finales pedidas
    columnas_finales = ['NP', 'DESCR', 'CATEGORIA', 'LINEA', 'VENTA', 'HITS', 'DEMANDA']
    df_final = resumen[columnas_finales].copy()
    
    # Ordenar por VENTA de mayor a menor para que el top salga arriba
    df_final.sort_values(by='VENTA', ascending=False, inplace=True)
    
    return df_final

# --- GENERACIÓN DE EXCEL CON DISEÑO ---
def formatear_excel_analisis(writer, df):
    workbook = writer.book
    worksheet = writer.sheets['ANALISIS GLOBAL']
    
    worksheet.freeze_panes(1, 0)
    worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
    
    # Formatos Corporativos
    fmt_header = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#10345C', 'font_color': 'white', 'border': 1})
    fmt_celdas_texto = workbook.add_format({'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3'})
    fmt_celdas_num = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3', 'num_format': '0'})
    
    # Escribir encabezados
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, fmt_header)
        
    # Anchos de columna
    worksheet.set_column('A:A', 20, fmt_celdas_texto) # NP
    worksheet.set_column('B:B', 45, fmt_celdas_texto) # DESCR
    worksheet.set_column('C:D', 25, fmt_celdas_texto) # CATEGORIA, LINEA
    worksheet.set_column('E:F', 15, fmt_celdas_num)   # VENTA, HITS
    worksheet.set_column('G:G', 15, fmt_celdas_texto) # DEMANDA

# --- INTERFAZ GRÁFICA ---
st.info("💡 Haz clic en el botón para consolidar todas las ventas (2025-2026) a nivel compañía y generar la base de análisis.")

if st.button("🚀 Ejecutar Análisis Global (1 Click)"):
    my_bar = st.progress(5, text="Iniciando protocolos de conexión...")
    
    df_analisis = procesar_analisis_global(my_bar)
    
    if df_analisis is not None and not df_analisis.empty:
        my_bar.progress(90, text="🎨 Generando Excel con diseño corporativo...")
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_analisis.to_excel(writer, sheet_name='ANALISIS GLOBAL', index=False)
            formatear_excel_analisis(writer, df_analisis)
            
        buffer.seek(0)
        fecha_str = datetime.datetime.now().strftime("%d_%m_%Y")
        name_file = f"Analisis_Demanda_Comercial_{fecha_str}.xlsx"
        
        my_bar.progress(95, text="☁️ Subiendo base de datos a Google Drive...")
        link = subir_excel_a_drive(buffer, name_file)
        
        my_bar.progress(100, text="✅ ¡Análisis completado!")
        
        if link:
            st.balloons()
            st.success(f"🎉 ¡Base de Análisis creada exitosamente! Contiene {len(df_analisis)} productos únicos.")
            st.markdown(f"### [📂 Abrir Base de Datos en Google Drive]({link})")
            
            st.write("📊 **Vista Previa del Top 10 Productos más Vendidos a Nivel Compañía:**")
            st.dataframe(df_analisis.head(10))
    else:
        st.error("No se pudo generar el análisis. Verifica que existan archivos de 2025/2026 en la carpeta origen.")
