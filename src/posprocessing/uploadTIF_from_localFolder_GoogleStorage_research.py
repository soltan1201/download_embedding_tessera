import os
import sys
import ee
import rasterio
import numpy as np
from pathlib import Path
import re                 # <--- Importar Regex
from affine import Affine # <--- Importar Affine
from google.cloud import storage
import time
import collections
collections.Callable = collections.abc.Callable

try:
    ee.Initialize(project= 'mapbiomas-brazil') # project='ee-cartassol'  #  'geo-data-s' # 'mapbiomas-caatinga-cloud02'
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


path_emb = "/run/media/superuser/Almacen/mapbiomas/embedding/DB/v1/2024"
path_tif = "/run/media/superuser/Almacen/mapbiomas/embedding/DB/v1"
path_base = Path('/run/media/superuser/Almacen/mapbiomas/embedding/DB/assets')
path_baseChange = '/run/media/superuser/Almacen/mapbiomas/embedding/DB/assets_corr'
CRS = "EPSG:32723"  # Tenha certeza que este é o CRS correto para TODOS os tiles
destination_blob_namefolder = "embedding_tif"
bucket_name = "mapbiomas-energia"



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


asset_embedding = "projects/mapbiomas-workspace/AMOSTRAS/col10/embedding_tessera"
col_emb_tessera = ee.ImageCollection(asset_embedding)
lst_id_Colemb = col_emb_tessera.reduceColumns(ee.Reducer.toList(3), ["longitud",  "latitud", "carta"]).get('list').getInfo()
lst_id_Colemb = [[float(kk[0]), float(kk[1]), kk[2]] for kk in lst_id_Colemb]
for cc, id in enumerate(lst_id_Colemb):
    print(f"#{cc} >>> {id}")

# sys.exit()
lstpathTIF = list(path_base.glob("*.txt"))
print(" loading tif in >>>>> \n >> ", path_base)
print(f"the folder have {len(lstpathTIF)}")
print("=============================================================")

# Crie a pasta de saída se não existir
output_assets_dir = os.path.join(path_base, "assets_tif")
os.makedirs(output_assets_dir, exist_ok=True)

for cc, line in enumerate(lstpathTIF[:]):
    name_file = line.stem
    partes = name_file.split("_")

    # grid_-46.35_-1.75_SA-23-V-D-IV_2024
    actual_lon = float(partes[1])
    actual_lat = float(partes[2])
    ids = str(partes[3])
    actual_year = partes[4]
    dirfolder = f"{partes[0]}_{actual_lon}_{actual_lat}"
    print(f"\n--- #{cc} >> {name_file} >> {dirfolder} ---")
    # grid_-35_95_-6_95_SB-24-Z-D-III_2024_g2d_tif
    # grid_-59_55_-7_35_SB-21-Y-C-II_2024_g2d_tif
    trio_procura = [actual_lon, actual_lat, ids]
    
    # --- 1. Parsear o .TXT ---
    doc_txt = open(line, "r")
    transform_str = "|" # String que guarda o transform
    if trio_procura not in lst_id_Colemb:    
        try:
            for ii, pp in enumerate(doc_txt):
                if ii == 0:
                    ppart = pp.split("|")
                    pparte = ppart[0].split(",")
                    height = int(pparte[0].replace("'", ""))
                    width = int(pparte[1])
                    count = int(pparte[2])
                    
                    # CORREÇÃO 3: Limpar o dtype
                    dtype_str = pparte[3].strip().replace("'", "")
                    
                    transform_str += "".join(ppart[1:])
                    transform_str = transform_str[:-1] + "|\n"
                else:
                    transform_str += "".join(pp)
            doc_txt.close()
            
            # CORREÇÃO 1: Criar o objeto Affine
            numeros = re.findall(r"[-+]?\d*\.\d+|[-+]?\d+", transform_str)
            if len(numeros) < 6:
                print(f"ERRO: Transform incompleto em {line.name}. Pulando.")
                continue
                
            transform_obj = Affine(float(numeros[0]), float(numeros[1]), float(numeros[2]),
                                float(numeros[3]), float(numeros[4]), float(numeros[5]))

        except Exception as e:
            print(f"ERRO ao ler .txt {line.name}: {e}")
            continue

        # --- 2. Carregar o .NPY ---
        # Bônus: Carregue o arquivo pelo nome exato, não por índice
        npy_filename = f"{dirfolder}.npy"
        npy_path = os.path.join(path_emb, dirfolder, npy_filename)

        if not os.path.exists(npy_path):
            print(f"ERRO: Arquivo .npy não encontrado em {npy_path}. Pulando.")
            continue

        filenpy = np.load(npy_path)
        
        # CORREÇÃO 2: Mover os eixos
        print(f"Shape original (H, W, C): {filenpy.shape}")
        filenpy_corrigido = np.moveaxis(filenpy, source=2, destination=0)
        print(f"Shape corrigido (C, H, W): {filenpy_corrigido.shape}")

        # --- 3. Salvar o GeoTIFF ---
        nome_output = f"grid_{actual_lon}_{actual_lat}_{ids}_{actual_year}.tif"
        output_path = os.path.join(output_assets_dir, nome_output)

        print(f"Salvando GeoTIFF com {count} bandas em {output_path}...")
    
        try:
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=height,
                width=width,
                count=count,
                dtype=dtype_str,        # <--- Usar dtype limpo
                crs=CRS,
                transform=transform_obj # <--- Usar objeto Affine
            ) as dst:
                dst.write(filenpy_corrigido) # <--- Usar array corrigido

            print("Arquivo salvo com sucesso!")

            source_tif_name_dest = os.path.join(path_baseChange, nome_output)
            comandoOS = f"gdal_translate {output_path} {source_tif_name_dest} -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co COMPRESS=LZW"
            os.system(comandoOS)

            destination_blob_name = f"{destination_blob_namefolder}/{nome_output.replace(".tif", "_g2d.tif")}"
            print(" destination >>> ", destination_blob_name)
            upload_to_gcs(bucket_name, source_tif_name_dest, destination_blob_name)
            # sys.exit()
            time.sleep(2)
            if os.path.exists(source_tif_name_dest):
                os.remove(output_path)
                os.remove(source_tif_name_dest)
                os.remove(npy_path)
                newpathnpy = npy_path.replace(".npy", "_scales.npy")
                os.remove(newpathnpy)
                name_file = npy_path.split("/")[-1].replace(".npy", ".tiff")
                os.remove(os.path.join(path_tif, name_file))

        
        except Exception as e:
            print(f"ERRO ao salvar GeoTIFF: {e}")