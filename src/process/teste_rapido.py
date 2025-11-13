
import os
import rasterio
import numpy as np
from geotessera import GeoTessera


# 1. Defina o caminho para seu diretório de cache personalizado
meu_diretorio_de_cache = "/run/media/superuser/Almacen/mapbiomas/embedding/DB"

# 2. Crie o diretório se ele não existir (opcional, mas boa prática)
os.makedirs(meu_diretorio_de_cache, exist_ok=True)

# 3. Passe o caminho para o construtor do GeoTessera
print(f"==== Inicializando GeoTessera com cache em: {meu_diretorio_de_cache} =====")
gt = GeoTessera(cache_dir=meu_diretorio_de_cache)

# Agora, qualquer chamada gt.fetch_embedding() salvará os arquivos
# dentro de "/home/superuser/meus_projetos/cache_tessera/geotessera/..."

lon = -51.35
lat = -31.55
year = 2024

try:
    embedding, crs, transform = gt.fetch_embedding(
        lon=lon,
        lat=lat, 
        year=year
    )
    print("\n==== Download concluído! ====")
    print(f"Shape original do array: {embedding.shape}")
    # 2. FAÇA A CORREÇÃO:
    # Rasterio espera (bandas, altura, largura)
    print(f"Shape original (H, W, C): {embedding.shape}")
    embedding_gis = np.moveaxis(embedding, source=2, destination=0)
    print(f"Shape para GIS (C, H, W): {embedding_gis.shape}")

    # --- 3. Coletar Metadados ---
    # O crs e o transform já vieram da função
    # Pegamos o resto do array corrigido
    count, height, width = embedding_gis.shape
    dtype = embedding_gis.dtype

    # --- 4. Salvar o arquivo GeoTIFF --- 2024/grid_-51.35_-31.55
    nome_output = f"grid_{lon}_{lat}_{year}.tif"
    output_path = os.path.join(meu_diretorio_de_cache, "assets", nome_output)

    print(f"Salvando GeoTIFF com {count} bandas em {output_path}...")

    # Usamos 'w' (write) para criar o novo arquivo
    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',       # Especifica o formato de saída
        height= height,        # Altura do raster (em pixels)
        width= width,          # Largura do raster (em pixels)
        count= count,          # Número de bandas (será 128)
        dtype= dtype,          # Tipo de dado (ex: float32)
        crs= crs,              # Sistema de Coordenadas (que veio do geotessera)
        transform= transform   # Transformação Afim (que veio do geotessera)
    ) as dst:
        # Escreve o array no arquivo
        dst.write(embedding_gis)

    print("Arquivo salvo com sucesso!")

except Exception as e:
    print(f"Erro: {e}")