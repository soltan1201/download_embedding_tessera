import os
import sys
import subprocess
from pathlib import Path

# --- 1. CONFIGURE AQUI ---

DIRETORIO_LOCAL = Path("/run/media/superuser/Almacen/mapbiomas/embedding/DB/assets")
GCS_BUCKET = "mapbiomas-energia" # (O nome do seu bucket)

# --- MUDANÇA AQUI ---
# 1. Defina o nome da sua "pasta" GEE
PASTA_ASSET_GEE = "embeddings_mapbiomas"

# 2. Defina o nome da "pasta" que você quer usar DENTRO do bucket GCS
#    (Não precisa criar antes, ela será criada automaticamente)
PASTA_NO_BUCKET_GCS = "embedding_tif" 

# 3. Combine o caminho completo do GEE
GEE_ASSET_FOLDER = f"projects/mapbiomas-workspace/AMOSTRAS/col10/embedding_tessera"

# --- FIM DA CONFIGURAÇÃO (O resto do script é igual) ---

# ... (resto do script) ...

def fazer_upload_para_gee(arquivo_tif, asset_id, bucket_gcs):
    # ...
    
    # O comando 'earthengine' é inteligente. 
    # Ao dar um asset_id como "projects/.../assets/pasta/arquivo",
    # ele automaticamente fará o upload para "gs://seu-bucket/pasta/arquivo.tif"
    # Mas para garantir, podemos especificar o caminho no bucket GCS
    # usando a opção --gcs_path_prefix

    command = [
        "earthengine", "upload", "image",
        "--asset_id", asset_id,
        "--bucket", bucket_gcs,
        
        # --- LINHA ADICIONAL ---
        # Esta linha força o GCS a usar sua pasta
        "--gcs_path_prefix", PASTA_NO_BUCKET_GCS, 
        
        f"--pyramiding_policy=mean",
        str(arquivo_tif.name) 
    ]
    print("> ", command)
    # sys.exit()
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"[SUCESSO] Tarefa iniciada para: {arquivo_tif.name}")
        print(f"  -> Upload para GCS em: gs://{bucket_gcs}/{PASTA_NO_BUCKET_GCS}/{arquivo_tif.name}")
        
    except subprocess.CalledProcessError as e:
        print(f"[ERRO] Falha ao iniciar tarefa para: {arquivo_tif.name}")
        print(f"  -> Erro: {e.stderr}")
    except FileNotFoundError:
        # ... (resto da função de erro) ...
        return False 
        
    return True

# ... (resto do script) ...
# name_tif = "grid_-50.95_-30.95_2024.tif"
# 1. Procura por TODOS os arquivos .tif no diretório
arquivos_tif = list(DIRETORIO_LOCAL.glob("*.tif"))

print(f"\nEncontrados {len(arquivos_tif)} arquivos .tif para upload.")

# 2. Inicia um LOOP (repetição)
for i, tif_file in enumerate(arquivos_tif[:2]):
    
    # 3. Aqui dentro, 'tif_file' é UM arquivo de cada vez
    nome_do_asset = tif_file.stem 
    asset_id_completo = f"{GEE_ASSET_FOLDER}/{nome_do_asset}"
    print(f"{nome_do_asset} >> {asset_id_completo}" )
    fazer_upload_para_gee(tif_file, asset_id_completo, GCS_BUCKET)