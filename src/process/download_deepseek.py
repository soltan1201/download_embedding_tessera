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
import hashlib
from pathlib import Path
import numpy as np

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

    return [min_lon, min_lat, max_lon, max_lat], [mean_lon, mean_lat]

def parse_registry_file_with_hash(file_path):
    """
    Analisa um arquivo de registro do GeoTessera e extrai os tiles disponíveis com hash.
    
    Formato esperado:
    2024/grid_-51.35_-31.55/grid_-51.35_-31.55.npy 30469beda70f1c609231f89c4e3bb97a3ce0c2a060c2c36c72808f60e2e25085
    """
    tiles = []
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Divide a linha em caminho e hash
                parts = line.split()
                if len(parts) < 2:
                    print(f"  ⚠️  Linha {line_num}: Formato inválido - {line}")
                    continue
                
                path = parts[0]
                file_hash = parts[1]
                
                # Extrai ano, lon e lat do caminho
                # Formato: 2024/grid_-51.35_-31.55/grid_-51.35_-31.55.npy
                match = re.search(r'(\d{4})/grid_(-?\d+\.\d+)_(-?\d+\.\d+)/', path)
                if match:
                    year = int(match.group(1))
                    lon = float(match.group(2))
                    lat = float(match.group(3))
                    
                    tile_info = {
                        'year': year,
                        'lon': lon,
                        'lat': lat,
                        'path': path,
                        'hash': file_hash,
                        'source_file': file_path.name,
                        'full_line': line
                    }
                    tiles.append(tile_info)
                    
                    if len(tiles) <= 3:  # Mostra apenas os primeiros 3
                        print(f"    ✅ {year}: lon={lon}, lat={lat}, hash={file_hash[:16]}...")
                else:
                    print(f"  ⚠️  Linha {line_num}: Padrão não reconhecido - {path}")
                    
    except Exception as e:
        print(f"  ❌ Erro ao ler arquivo {file_path}: {e}")
    
    return tiles

def check_local_cache_for_hash(file_hash):
    """
    Verifica se o arquivo com o hash específico já está em cache local
    """
    cache_dir = Path.home() / '.cache' / 'geotessera'
    
    # Procura recursivamente por arquivos que possam conter este hash no nome
    potential_files = []
    for pattern in ['**/*.npy', '**/*.txt', '**/*.json']:
        potential_files.extend(cache_dir.glob(pattern))
    
    for file_path in potential_files:
        # Verifica se o hash está no nome do arquivo ou no conteúdo
        if file_hash in file_path.name:
            return file_path
        
        # Se for arquivo de texto, verifica o conteúdo
        if file_path.suffix == '.txt':
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    if file_hash in content:
                        return file_path
            except:
                continue
    
    return None

def download_with_hash_fallback(lon, lat, year, expected_hash=None):
    """
    Tenta baixar o embedding usando coordenadas e, opcionalmente, valida com hash
    """
    try:
        print(f"📥 Baixando tile: ano={year}, lon={lon}, lat={lat}")
        embedding, crs, transform = gt.fetch_embedding(
            lon=lon,
            lat=lat, 
            year=year
        )
        
        # Se temos um hash esperado, podemos validar o embedding
        if expected_hash:
            # Calcula hash do embedding baixado para validação
            embedding_hash = calculate_embedding_hash(embedding)
            if embedding_hash == expected_hash:
                print(f"✅ Hash validado: {expected_hash[:16]}...")
            else:
                print(f"⚠️  Hash não coincide. Esperado: {expected_hash[:16]}..., Obtido: {embedding_hash[:16]}...")
        
        return True, embedding.shape, embedding, crs, transform
    except Exception as e:
        print(f"❌ Erro ao baixar tile: {e}")
        return False, None, None, None, None

def calculate_embedding_hash(embedding):
    """
    Calcula hash SHA-256 de um array numpy para validação
    """
    try:
        # Converte o array para bytes e calcula hash
        embedding_bytes = embedding.tobytes()
        return hashlib.sha256(embedding_bytes).hexdigest()
    except Exception as e:
        print(f"❌ Erro ao calcular hash: {e}")
        return None

def search_tiles_by_hash(all_tiles, target_hash):
    """
    Busca tiles por hash específico
    """
    matching_tiles = []
    
    for tile in all_tiles:
        if tile['hash'] == target_hash:
            matching_tiles.append(tile)
    
    return matching_tiles

def find_tiles_for_location(all_tiles, target_lon, target_lat, target_year=None, tolerance=0.1):
    """
    Encontra tiles disponíveis para uma localização específica
    """
    matching_tiles = []
    
    for tile in all_tiles:
        lon_match = abs(tile['lon'] - target_lon) <= tolerance
        lat_match = abs(tile['lat'] - target_lat) <= tolerance
        year_match = target_year is None or tile['year'] == target_year
        
        if lon_match and lat_match and year_match:
            matching_tiles.append(tile)
    
    return matching_tiles

# Parâmetros principais
param = {
    'cartas_shp': "projects/mapbiomas-workspace/AUXILIAR/CARTAS_IBGE/articulacao_100000_mapbiomas"
}

# PRIMEIRO: Carregar todos os tiles do registry com hash
print("🔄 Carregando todos os tiles disponíveis do registry...")
registry_dir = Path.home() / '.cache' / 'geotessera' / 'tessera-manifests' / 'registry' / 'embeddings'
all_tiles = []

if registry_dir.exists():
    registry_files = list(registry_dir.glob('*.txt'))
    print(f"📄 Encontrados {len(registry_files)} arquivos de registro")
    
    for reg_file in registry_files:
        print(f"📖 Lendo {reg_file.name}...")
        tiles = parse_registry_file_with_hash(reg_file)
        all_tiles.extend(tiles)
        print(f"  📊 {len(tiles)} tiles encontrados neste arquivo")
else:
    print("❌ Diretório de registry não encontrado")
    sys.exit(1)

print(f"✅ Total de {len(all_tiles)} tiles disponíveis no registry")

# Exemplo: Buscar tile específico pelo hash que você mencionou
target_hash = "30469beda70f1c609231f89c4e3bb97a3ce0c2a060c2c36c72808f60e2e25085"
print(f"\n🔍 Buscando tile com hash: {target_hash[:16]}...")

matching_hash_tiles = search_tiles_by_hash(all_tiles, target_hash)
if matching_hash_tiles:
    tile_info = matching_hash_tiles[0]
    print(f"✅ Tile encontrado!")
    print(f"   📅 Ano: {tile_info['year']}")
    print(f"   📍 Longitude: {tile_info['lon']}")
    print(f"   📍 Latitude: {tile_info['lat']}")
    print(f"   🗂️  Path: {tile_info['path']}")
    print(f"   🔑 Hash: {tile_info['hash'][:16]}...")
    
    # Verificar se já está em cache local
    cached_file = check_local_cache_for_hash(target_hash)
    if cached_file:
        print(f"💾 Arquivo encontrado em cache: {cached_file}")
    else:
        print("📥 Arquivo não encontrado em cache local")
else:
    print("❌ Nenhum tile encontrado com este hash")

# Carrega shapes das cartas
try:
    shps_cartas = ee.FeatureCollection(param['cartas_shp'])
    lst_ids = shps_cartas.reduceColumns(ee.Reducer.toList(), ['indNomencl']).get('list').getInfo()
    print(f"📊 Carregadas {len(lst_ids)} cartas IBGE")
    
except Exception as e:
    print(f"❌ Erro ao carregar shapes: {e}")
    sys.exit(1)

# Processa cada carta usando as informações de hash
nyear = 2024
max_cartas = 5  # Limite para testes

print(f"\n🎯 Processando {max_cartas} cartas com suporte a hash...")

cartas_com_embedding = 0
cartas_sem_embedding = 0
tiles_validados_hash = 0

for cc, ids in enumerate(lst_ids[:max_cartas]):
    print(f"\n{'='*60}")
    print(f"🗺️  Processando carta #{cc+1}/{min(max_cartas, len(lst_ids))}: {ids}")
    
    try:
        # Obtém geometria da carta
        shptmp = shps_cartas.filter(ee.Filter.eq('indNomencl', ids)).geometry().bounds()
        listCoord = shptmp.getInfo()['coordinates'][0]
        
        box_Coords, coord_center = get_list_coord(listCoord)
        center_lon = round(coord_center[0], 2)
        center_lat = round(coord_center[1], 2)
        
        print(f"📍 Bounding Box: {[round(c, 4) for c in box_Coords]}")
        print(f"🎯 Centroide: ({center_lon}, {center_lat})")
        print(f"📅 Ano alvo: {nyear}")

        # Busca tiles disponíveis para esta localização
        matching_tiles = find_tiles_for_location(all_tiles, center_lon, center_lat, nyear, tolerance=0.5)
        
        if matching_tiles:
            print(f"✅ Encontrados {len(matching_tiles)} tiles para esta área")
            
            # Ordena por proximidade ao centroide
            matching_tiles.sort(key=lambda x: abs(x['lon'] - center_lon) + abs(x['lat'] - center_lat))
            
            # Baixa o tile mais próximo COM validação de hash
            best_tile = matching_tiles[0]
            print(f"🎯 Tile selecionado: ano={best_tile['year']}, lon={best_tile['lon']}, lat={best_tile['lat']}")
            print(f"🔑 Hash do tile: {best_tile['hash'][:16]}...")
            
            success, shape, embedding, crs, transform = download_with_hash_fallback(
                best_tile['lon'], 
                best_tile['lat'], 
                best_tile['year'],
                expected_hash=best_tile['hash']
            )
            
            if success:
                print(f"✅ Tile baixado com sucesso!")
                print(f"   📐 Shape: {shape}")
                print(f"   📌 CRS: {crs}")
                
                cartas_com_embedding += 1
                tiles_validados_hash += 1
                
                # Salvar o embedding com informações de hash
                output_dir = Path("embeddings_validados")
                output_dir.mkdir(exist_ok=True)
                
                filename = f"embedding_{best_tile['year']}_lon{best_tile['lon']}_lat{best_tile['lat']}_hash{best_tile['hash'][:16]}.npy"
                output_path = output_dir / filename
                
                np.save(output_path, embedding)
                print(f"💾 Embedding salvo: {output_path}")
                
            else:
                print("❌ Falha no download do tile")
                cartas_sem_embedding += 1
                
        else:
            print("❌ Nenhum tile encontrado para esta localização/ano")
            cartas_sem_embedding += 1
                
    except Exception as e:
        print(f"⚠️  Erro processando carta {ids}: {e}")
        cartas_sem_embedding += 1
        continue

# Estatísticas finais
print(f"\n{'='*60}")
print("📊 RESUMO DO PROCESSAMENTO COM HASH")
print(f"{'='*60}")
print(f"📈 Total de cartas processadas: {max_cartas}")
print(f"✅ Cartas COM embedding: {cartas_com_embedding}")
print(f"❌ Cartas SEM embedding: {cartas_sem_embedding}")
print(f"🔐 Tiles validados por hash: {tiles_validados_hash}")
print(f"📊 Taxa de sucesso: {(cartas_com_embedding/max_cartas)*100:.1f}%")

# Informações sobre o hash específico que você mencionou
print(f"\n🔍 INFORMAÇÕES DO HASH ESPECÍFICO:")
print(f"   Hash: {target_hash}")
if matching_hash_tiles:
    tile = matching_hash_tiles[0]
    print(f"   📅 Ano: {tile['year']}")
    print(f"   📍 Coordenadas: lon={tile['lon']}, lat={tile['lat']}")
    print(f"   🗂️  Path: {tile['path']}")
    print(f"   📄 Arquivo de registro: {tile['source_file']}")
    
    # Sugestão de uso direto
    print(f"\n💡 USO DIRETO DESTE TILE:")
    print(f"   embedding, crs, transform = gt.fetch_embedding(")
    print(f"       lon={tile['lon']},")
    print(f"       lat={tile['lat']},") 
    print(f"       year={tile['year']}")
    print(f"   )")

print("\n🎉 Processamento concluído!")