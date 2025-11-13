#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""

import ee
import os
import sys

import collections
import json
import subprocess
import re
from pathlib import Path
import numpy as np
import rasterio
# Corrige compatibilidade entre Python 2 e 3
collections.Callable = collections.abc.Callable

from geotessera import GeoTessera

# Inicializa o cliente GeoTessera
try:
    gt = GeoTessera()
    print("==== inicializou o GeoTessera =======")
except Exception as e:
    print(f"Erro ao inicializar GeoTessera: {e}")
    sys.exit(1)

# Configura caminhos e importações
pathparent = str(Path(os.getcwd()).parent)
sys.path.append(pathparent)

try:
    from configure_account_projects_ee import get_current_account, get_project_from_account
    from gee_tools import *
except ImportError as e:
    print(f"Erro importando módulos: {e}")
    sys.exit(1)

# Inicializa Earth Engine
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error: {e}")
    sys.exit(1)


# 1. Defina o caminho para seu diretório de cache personalizado
meu_diretorio_de_cache = "/run/media/superuser/Almacen/mapbiomas/embedding/DB"

# 2. Crie o diretório se ele não existir (opcional, mas boa prática)
os.makedirs(meu_diretorio_de_cache, exist_ok=True)
# 3. Passe o caminho para o construtor do GeoTessera
print(f"==== Inicializando GeoTessera com cache em: {meu_diretorio_de_cache} =====")
gt = GeoTessera(cache_dir=meu_diretorio_de_cache)

def get_list_coord(mlist_coord):
    """Calcula bounding box e centroide a partir de lista de coordenadas"""
    min_lon = 4000
    min_lat = 4000
    max_lon = -4000
    max_lat = -4000
    
    for paircoord in mlist_coord:
        lon, lat = paircoord[0], paircoord[1]
        min_lon = min(min_lon, lon)
        max_lon = max(max_lon, lon)
        min_lat = min(min_lat, lat)
        max_lat = max(max_lat, lat)

    mean_lon = (min_lon + max_lon) / 2
    mean_lat = (min_lat + max_lat) / 2

    min_lon = round(min_lon, 2)
    max_lon = round(max_lon, 2)
    # print(min_lon, max_lon)
    lstCoordlon = []
    coordlon = min_lon - 0.05
    lstCoordlon.append(coordlon)
    # print(coordlon)
    while coordlon < max_lon + 0.01:
        coordlon += 0.1
        coordlon = round(coordlon, 2)
        # print(coordlon)
        lstCoordlon.append(coordlon)
    # print(lstCoordlon)

    min_lat = round(min_lat, 2)
    max_lat = round(max_lat, 2)
    # print(min_lat, max_lat)
    lstCoordlat = []
    coordlat = min_lat - 0.05
    lstCoordlat.append(coordlat)
    # print(coordlat)
    while coordlat < max_lat + 0.01:
        coordlat += 0.1
        coordlat = round(coordlat, 2)
        # print(coordlat)
        lstCoordlat.append(coordlat)
    # print(lstCoordlat)
    lstparCoord = []
    for nlon in lstCoordlon:
        for nlat in lstCoordlat:
            # print([nlon, nlat])
            lstparCoord.append([nlon, nlat])

    # sys.exit()
    return [min_lon, min_lat, max_lon, max_lat], [mean_lon, mean_lat], lstparCoord

def load_available_tiles_from_embeddings_dir():
    """Carrega todos os tiles disponíveis do diretório de embeddings"""
    embeddings_dir = Path.home() / '.cache' / 'geotessera' / 'tessera-manifests' / 'registry' / 'embeddings'
    available_tiles = set()
    
    print(f"🔍 Procurando embeddings em: {embeddings_dir}")
    
    if not embeddings_dir.exists():
        print("❌ Diretório de embeddings não encontrado")
        return available_tiles
    
    # Procura por arquivos TXT de embeddings
    embedding_files = list(embeddings_dir.glob('embeddings_*.txt'))
    print(f"📄 Encontrados {len(embedding_files)} arquivos de embedding")
    
    # Padrão regex para extrair ano, lon e lat do nome do arquivo
    # Formato: embeddings_2017_lon-10_lat55.txt
    pattern = r'embeddings_(\d+)_lon(-?\d+)_lat(-?\d+)\.txt'
    
    for embed_file in embedding_files:
        try:
            match = re.search(pattern, embed_file.name)
            if match:
                year = int(match.group(1))
                lon = int(match.group(2))
                lat = int(match.group(3))
                
                tile_key = (year, lat, lon)  # (year, lat, lon)
                available_tiles.add(tile_key)
                
                if len(available_tiles) <= 5:  # Mostra apenas os primeiros 5
                    print(f"   ✅ {embed_file.name} -> ano: {year}, lat: {lat}, lon: {lon}")
            else:
                print(f"   ⚠️  Padrão não reconhecido: {embed_file.name}")
                
        except Exception as e:
            print(f"⚠️  Erro ao processar {embed_file.name}: {e}")
            continue
    
    print(f"✅ Total de {len(available_tiles)} tiles disponíveis identificados")
    return available_tiles

def check_tile_in_embeddings(available_tiles, lon, lat, year, tolerance=0.01):
    """Verifica se um tile específico está nos embeddings com uma tolerância para coordenadas"""
    for tile_year, tile_lat, tile_lon in available_tiles:
        if (int(tile_year) == int(year) and  # Permite diferença de 1 ano
            abs(tile_lat - lat) <= tolerance and
            abs(tile_lon - lon) <= tolerance):
            return True, (tile_year, tile_lat, tile_lon)
    return False, None

def find_available_years_in_embeddings(available_tiles, lon, lat, tolerance=0.01):
    """Encontra todos os anos disponíveis para uma localização nos embeddings"""
    available_years = []
    for tile_year, tile_lat, tile_lon in available_tiles:
        if abs(tile_lat - lat) <= tolerance and abs(tile_lon - lon) <= tolerance:
            available_years.append((tile_year, tile_lat, tile_lon))
    return available_years

def load_embedding_from_file(lon, lat, year, listaCoord):
    """Tenta carregar o embedding diretamente do arquivo local"""
    embeddings_dir = Path.home() / '.cache' / 'geotessera' / 'tessera-manifests' / 'registry' / 'embeddings'
    
    # Procura por arquivos que correspondam ao padrão
    pattern = f"embeddings_{year}_lon{lon}_lat{lat}.txt"
    exact_file = embeddings_dir / pattern  
    
    if exact_file.exists():
        # print("entrou")
        try:
            # Lê o arquivo de embedding
            print("path read \n >>> ", exact_file)
            textdata = open(exact_file)
            dict_grid = {}
            for cc, line in enumerate(textdata):
                line = line[:-1]  
                parte = line.split(" ")
                dict_grid[parte[0].split('/')[1]] = parte                   
                    # if itemSearch in str(parte[0]):
                    #     print(line)
                    #     print("=======================") 
            lstKeysGrid = list(dict_grid.keys())
            lstpartes = []
            for parC in listaCoord:
                itemSearch = f"grid_{parC[0]}_{parC[1]}"      
                # print(itemSearch)
                if itemSearch in lstKeysGrid:
                    # print("Achoue")
                    lstpartes.append(dict_grid[itemSearch])
            # print(lstpartes)

            return True, lstpartes
        except Exception as e:
            print(f"❌ Erro ao ler arquivo {exact_file}: {e}")
            return False, []
    
    # # Se não encontrou exato, procura com tolerância
    # embedding_files = list(embeddings_dir.glob(f'embeddings_{year}_lon*_lat*.txt'))
    
    # for embed_file in embedding_files:
    #     try:
    #         match = re.search(r'embeddings_(\d+)_lon(-?\d+)_lat(-?\d+)\.txt', embed_file.name)
    #         if match:
    #             file_year = int(match.group(1))
    #             file_lon = int(match.group(2))
    #             file_lat = int(match.group(3))
                
    #             if (abs(file_lon - lon) <= 0.01 and abs(file_lat - lat) <= 0.01):
    #                 embedding_data = np.loadtxt(embed_file)
    #                 print(f"✅ Embedding carregado localmente: {embed_file.name}")
    #                 return True, embedding_data.shape, embedding_data
    #     except Exception as e:
    #         continue
    
    return False, []

def get_tile_directly(lon, lat, year):
    """Tenta buscar o tile via API (fallback)"""
    try:
        embedding, crs, transform = gt.fetch_embedding(lon=lon, lat=lat, year=year)
        return True, embedding.shape, embedding
    except Exception as e:
        if "not available" in str(e).lower() or "not found" in str(e).lower():
            return False, None, None
        else:
            raise e

def arredondar_ao5_proximo(numero):
    """
    Arredonda um número inteiro para a dezena mais próxima.
    
    Exemplo:
    51 -> 50 + 5
    56 -> 60 
    """
    if not isinstance(numero, int):
        # Garante que a entrada é um inteiro para o cálculo funcionar
        try:
            numero = int(numero)
        except ValueError:
            return "Erro: Entrada deve ser um número inteiro."

    # A ideia é dividir por 10, arredondar o resultado e multiplicar por 10 novamente.
    # Ex: 51 / 10 = 5.1 -> round(5.1) = 5 -> 5 * 10 = 50
    # Ex: 56 / 10 = 5.6 -> round(5.6) = 6 -> 6 * 10 = 60
    
    num_arred = round(numero / 10) * 10
    # grantindo que (-51.25, -31.75) ~ (-55.0, -35.0)
    if abs(num_arred) < abs(numero):        
        num_arred -= 5
            
    return num_arred


# Parâmetros principais
param = {
    'cartas_shp': "projects/mapbiomas-workspace/AUXILIAR/CARTAS_IBGE/articulacao_100000_mapbiomas"
}

# PRIMEIRO: Carregar todos os tiles disponíveis do diretório de embeddings
print("🔄 Carregando tiles disponíveis do diretório de embeddings...")
available_tiles = load_available_tiles_from_embeddings_dir()
# for ii in available_tiles:
#     if ii[0] == 2024:
#         print(ii)
# sys.exit()

# Se não encontrou tiles, tentar via CLI como fallback
if not available_tiles:
    print("🔄 Tentando obter cobertura via CLI...")
    try:
        result = subprocess.run(['geotessera', 'coverage', '--country', 'br', '--format', 'json'], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            coverage_data = json.loads(result.stdout)
            print("✅ Cobertura obtida via CLI")
            
            # Processa os dados da CLI para o formato padrão
            if 'tiles' in coverage_data:
                for tile in coverage_data['tiles']:
                    if 'year' in tile and 'lat' in tile and 'lon' in tile:
                        tile_key = (tile['year'], tile['lat'], tile['lon'])
                        available_tiles.add(tile_key)
            print(f"✅ Adicionados {len(available_tiles)} tiles da CLI")
    except Exception as e:
        print(f"❌ Erro ao obter cobertura via CLI: {e}")

# Carrega shapes das cartas
try:
    shps_cartas = ee.FeatureCollection(param['cartas_shp'])
    lst_ids = shps_cartas.reduceColumns(ee.Reducer.toList(), ['indNomencl']).get('list').getInfo()
    print(f"📊 Carregadas {len(lst_ids)} cartas IBGE")
    
except Exception as e:
    print(f"❌ Erro ao carregar shapes: {e}")
    sys.exit(1)

# Processa cada carta
nyear = 2024
max_cartas = 90  # Limite para testes
inic_cartas = 72
print(f"\n🎯 Processando {max_cartas -  inic_cartas} cartas...")
path_emb = "/run/media/superuser/Almacen/mapbiomas/embedding/DB/v1"
cartas_com_embedding = 0
cartas_sem_embedding = 0
cartas_carregadas_local = 0

for cc, ids in enumerate(lst_ids[inic_cartas:max_cartas]):
    print(f"\n{'='*60}")
    print(f"🗺️  Processando carta #{cc + inic_cartas + 1}/{ len(lst_ids)}: {ids}")
    
    try:
        # Obtém geometria da carta
        shptmp = shps_cartas.filter(ee.Filter.eq('indNomencl', ids)).geometry().bounds()
        listCoord = shptmp.getInfo()['coordinates'][0]
        
        box_Coords, coord_center, listparCoord = get_list_coord(listCoord)
        extLon = round(coord_center[0], 2)
        extLonint = arredondar_ao5_proximo(extLon)
        extLat = round(coord_center[1], 2)
        extLatint = arredondar_ao5_proximo(extLat)
        
        print(f"📍 Bounding Box: {[round(c, 4) for c in box_Coords]}")
        print(f"🎯 Centroide: ({extLon}, {extLat})")
        print(f"🎯 Centroide in int: ({extLonint}, {extLatint})")
        print(f"📅 Ano alvo: {nyear}")

        # PRIMEIRO: Verifica nos EMBEDDINGS LOCAIS se o tile está disponível
        tile_found, exact_coords = check_tile_in_embeddings(available_tiles, extLonint, extLatint, nyear)
        # print(tile_found)
        # print(exact_coords)
        # sys.exit()
        if tile_found:
            actual_year, actual_lat, actual_lon = exact_coords
            print(f"✅ Tile encontrado localmente! (ano: {actual_year}, lat: {actual_lat}, lon: {actual_lon})")
            
            # Tenta carregar o embedding do arquivo local
            local_loaded, list_local_embedding = load_embedding_from_file(actual_lon, actual_lat, actual_year, listparCoord)
            print(f"Arquivo localizado {local_loaded} com coordenadas {len(list_local_embedding)}")
            

            ## Se não conseguiu carregar localmente, tenta via API
            print("🔄 Tentando via API...")
            for ii, file_emb in enumerate(list_local_embedding):
                print(f"#{ii} >> {file_emb[0]}")
                partes = file_emb[0].split("/")[1]
                partes = partes.split("_")
                actual_lon = float(partes[1])
                actual_lat = float(partes[2])                
                print(f"{actual_year} >> {actual_lon} | {actual_lat}")

                # grid_-52.45_-31.95.tiff
                file_exit = os.path.join(path_emb, f"grid_{actual_lon}_{actual_lat}.tiff")
                if not os.path.exists(file_exit):  
                    try:
                        embedding, crs, transform = gt.fetch_embedding(
                            lon= actual_lon, 
                            lat= actual_lat, 
                            year= actual_year
                        )
                        print("\n==== Download concluído! ====")
                        print(f"✅ Download via API - Shape: {embedding.shape}")
                        print(f"📌 CRS: {crs}")


                        cartas_com_embedding += 1
                        # 2. FAÇA A CORREÇÃO:
                        # Rasterio espera (bandas, altura, largura)
                        # print(f"Shape original (H, W, C): {embedding.shape}")
                        # embedding_gis = np.moveaxis(embedding, source=2, destination=0)
                        # print(f"Shape para GIS (C, H, W): {embedding_gis.shape}")
                        
                        # --- 3. Coletar Metadados ---
                        # O crs e o transform já vieram da função
                        # Pegamos o resto do array corrigido
                        height, width, count = embedding.shape
                        dtype = embedding.dtype

                        print("===== transform ====\n", transform)
                        file_content = f"{height},{width},{count},{dtype},{transform}"
                        nome_output = f"grid_{actual_lon}_{actual_lat}_{ids}_{actual_year}.txt"
                        output_path = os.path.join(meu_diretorio_de_cache, "assets", nome_output)
                        with open(output_path, "w") as file:
                            file.write(file_content)


                        # # --- 4. Salvar o arquivo GeoTIFF --- 2024/grid_-51.35_-31.55
                        # nome_output = f"grid_{actual_lon}_{actual_lat}_{ids}_{actual_year}.tif"
                        # output_path = os.path.join(meu_diretorio_de_cache, "assets", nome_output)

                        # print(f"Salvando GeoTIFF com {count} bandas em {output_path}...")
                        # # Usamos 'w' (write) para criar o novo arquivo
                        # with rasterio.open(
                        #     output_path,
                        #     'w',
                        #     driver='GTiff',       # Especifica o formato de saída
                        #     height= height,        # Altura do raster (em pixels)
                        #     width= width,          # Largura do raster (em pixels)
                        #     count= count,          # Número de bandas (será 128)
                        #     dtype= dtype,          # Tipo de dado (ex: float32)
                        #     crs= crs,              # Sistema de Coordenadas (que veio do geotessera)
                        #     transform= transform   # Transformação Afim (que veio do geotessera)
                        # ) as dst:
                        #     # Escreve o array no arquivo
                        #     dst.write(embedding_gis)

                        print("Arquivo salvo com sucesso!")
                    
                    except Exception as e:
                        print(f"❌ Erro no download via API: {e}")
                        cartas_sem_embedding += 1

                else:
                    print("File exist <>> ", f"grid_{actual_lon}_{actual_lat}.tiff")

            # sys.exit() 
        
    except Exception as e:
        print(f"⚠️  Erro processando carta {ids}: {e}")
        cartas_sem_embedding += 1
        continue

# Estatísticas finais
print(f"\n{'='*60}")
print("📊 RESUMO DO PROCESSAMENTO")
print(f"{'='*60}")
print(f"📈 Total de cartas processadas: {max_cartas}")
print(f"💾 Embeddings carregados LOCALMENTE: {cartas_carregadas_local}")
print(f"✅ Cartas COM embedding: {cartas_com_embedding}")
print(f"❌ Cartas SEM embedding: {cartas_sem_embedding}")
print(f"📊 Taxa de sucesso: {(cartas_com_embedding/max_cartas)*100:.1f}%")

# if available_tiles:
#     print(f"\n📁 Estatísticas dos embeddings locais:")
#     print(f"   - Total de tiles disponíveis: {len(available_tiles)}")
    
#     # Conta por ano
#     years_count = {}
#     for year, lat, lon in available_tiles:
#         years_count[year] = years_count.get(year, 0) + 1
    
#     for year, count in sorted(years_count.items()):
#         print(f"   - Ano {year}: {count} tiles")

print("\n🎉 Processamento concluído!")