from google.cloud import storage

# --- 1. CONFIGURAÇÃO ---

# O nome exato do seu bucket
BUCKET_NAME = "mapbiomas-energia"

# O caminho para sua chave de serviço (a mesma do script de upload)
_json_key_path = '/home/superuser/Dados/mapbiomas/mykeys/mapbiomas-agua-36521f541610.json'
_project_name = 'mapbiomas-agua'

# Lista dos arquivos exatos (blobs) para deletar
# NOTA: Estes são os nomes DENTRO do bucket (sem "gs://...")
# Os seus arquivos parecem estar sem a extensão .tif, como discutimos.
files_to_delete = [
    "embedding_tif/grid_-51.65_-31.15_2024",
    "embedding_tif/grid_-51.85_-31.15_2024",
    "embedding_tif/grid_-52.05_-31.75_2024",
    "embedding_tif/grid_-52.35_-31.55_2024",
    "embedding_tif/grid_-52.45_-31.75_2024"
]

# --- 2. FUNÇÃO DE DELEÇÃO ---

def delete_blob(bucket_name, blob_name):
    """Deleta um arquivo (blob) específico do bucket."""
    try:
        # Inicializa o cliente com a chave de serviço
        storage_client = storage.Client.from_service_account_json(
            _json_key_path, project=_project_name
        )
        
        # Pega o bucket
        bucket = storage_client.bucket(bucket_name)
        
        # Pega o arquivo (blob)
        blob = bucket.blob(blob_name)
        
        # Tenta deletar
        print(f"Tentando deletar: gs://{bucket_name}/{blob_name} ...")
        blob.delete()
        print(f"{blob_name} Arquivo deletado.")
        
    except Exception as e:
        # Trata erros (ex: arquivo não existe, permissão negada)
        print(f"[ERRO] Falha ao deletar {blob_name}: {e}")

# --- 3. EXECUÇÃO ---

print(f"--- Iniciando deleção de {len(files_to_delete)} arquivos do bucket {BUCKET_NAME} ---")

for file_path in files_to_delete:
    delete_blob(BUCKET_NAME, file_path)

print("--- Script concluído ---")