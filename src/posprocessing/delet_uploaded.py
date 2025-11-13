
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""
import os
import subprocess

# 1. Defina o caminho para seu diretório de cache personalizado
meu_diretorio_de_cache = "/run/media/superuser/Almacen/mapbiomas/embedding/DB"
remover = False
gcBucket= "mapbiomas-energia"
folderDirGS = 'embedding_tif/'
comando = f"gcloud storage ls gs://{gcBucket}/{folderDirGS}*"
lstdirsFileGS = os.system(comando)  # os.system
print(lstdirsFileGS)
processo = subprocess.check_output(comando, shell=True)

lstdirsFileGS = str(processo.decode('utf-8')).split("\n")
nyear = 2024
prefixo = f"gs://{gcBucket}/{folderDirGS}/"

for cc, nfile in enumerate(lstdirsFileGS):    
    name = nfile.split("/")[-1].replace('_g2d', '')
    print(f"# {cc} >> {name}")
    dirAsset_tmp = os.path.join(meu_diretorio_de_cache, 'assets', name)
    if os.path.exists(dirAsset_tmp) and remover:
        os.remove(dirAsset_tmp)
        print(dirAsset_tmp, '.. removido')

    dirAsset_tmpCorr = os.path.join(meu_diretorio_de_cache, 'assets_corr', name)
    if os.path.exists(dirAsset_tmpCorr) and remover:
        os.remove(dirAsset_tmpCorr)
        print(dirAsset_tmpCorr, '.. removido')