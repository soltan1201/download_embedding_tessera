from google.cloud import storage
import os
import time
import sys
from pathlib import Path
# Example usage
bucket_name = "mapbiomas-energia"
path_base = Path('/run/media/superuser/Almacen/mapbiomas/embedding/DB/assets')
path_baseChange = '/run/media/superuser/Almacen/mapbiomas/embedding/DB/assets_corr'
source_file_name = "./log_files_saved.txt"
destination_blob_namefolder = "embedding_tif"
# 3. Combine o caminho completo do GEE
GEE_ASSET_FOLDER = "projects/mapbiomas-workspace/AMOSTRAS/col10/embedding_tessera"
path_emb = "/run/media/superuser/Almacen/mapbiomas/embedding/DB/v1/2024"
nyear = 2024
from_asset = False

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage using a service account key.
    
    Args:
        json_key_path (str): Path to the JSON key file.
        project_name (str): Google Cloud project name.
        bucket_name (str): Name of the GCS bucket.
        source_file_name (str): Path to the local file to upload.
        destination_blob_name (str): Name of the object in the bucket.
    """
    _json_key_path = '/home/superuser/Dados/mapbiomas/mykeys/mapbiomas-agua-36521f541610.json'
    _project_name = 'mapbiomas-agua'
    # Define um timeout longo para o upload (em segundos)
    # 5 minutos = 300 segundos
    UPLOAD_TIMEOUT = 300
    try:
        # Initialize a storage client using the service account
        storage_client = storage.Client.from_service_account_json(_json_key_path, project=_project_name)

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object (path in the bucket)
        blob = bucket.blob(destination_blob_name)

        print(f"File {source_file_name} \n >>>>>>>>> uploading to {destination_blob_name}")

        # --- A MUDANÇA ESTÁ AQUI ---
        # Adicionamos o parâmetro 'timeout'
        blob.upload_from_filename(source_file_name, timeout=UPLOAD_TIMEOUT)

        print(f"File {source_file_name} \n >>>>>>>>> uploaded to {destination_blob_name}")
    except Exception as e:
        print(f"\n!!!!!!!!!!!!!! ERRO NO UPLOAD !!!!!!!!!!!!!!")
        print(f"  Arquivo: {source_file_name}")
        print(f"  Erro: {e}")
        print("  O script continuará com o próximo arquivo.")

print("=============================================================")
# lstpathTIF = glob.glob(os.path.join(path_base, '*.tif'))
if from_asset:
    lstpathTIF = list(path_base.glob("*.tif"))
    print(" loading tif in >>>>> \n >> ", path_base)
else:
    lstpathTIF = list(Path(path_baseChange).glob("*.tif"))
    print(" loading tif in >>>>> \n >> ", path_baseChange)

print(len(lstpathTIF))
print("=============================================================")
# Cria o diretório de destino se ele não existir
if not os.path.exists(path_baseChange):
    os.makedirs(path_baseChange)



for cc, namepath in enumerate(lstpathTIF[:]):
    print(namepath)
    if  cc  > -1:
        source_file_name = namepath
        nameFileTIF = namepath.stem
        # print(f"{nameFileTIF}  \n >> {source_file_name}")
        
        source_tif_name_dest = os.path.join(path_baseChange, nameFileTIF + ".tif")
        # change configuration TIF compressao The required TIFF tag 'TileWidth' is not present in the IFD at index 0
        # https://github.com/cogeotiff/cog-spec/blob/master/spec.md
        if from_asset:            
            comandoOS = f"gdal_translate {source_file_name} {source_tif_name_dest} -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=LZW"
            os.system(comandoOS)

        #################################################################
        print(f" # {cc}/{len(lstpathTIF)}   > ...{nameFileTIF} ") 
        destination_blob_name = f"{destination_blob_namefolder}/{nameFileTIF}_g2d.tif"
        print(" destination >>> ", destination_blob_name)
        upload_to_gcs(bucket_name, source_tif_name_dest, destination_blob_name)
        # sys.exit()
        time.sleep(2)
        if os.path.exists(source_tif_name_dest):
            os.remove(source_file_name)