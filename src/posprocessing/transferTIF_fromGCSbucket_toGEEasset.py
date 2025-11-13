
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""
import os
import subprocess
import ee
import time
import sys
import json
import collections
from pathlib import Path
collections.Callable = collections.abc.Callable

try:
    ee.Initialize(project= 'mapbiomas-brazil') # project='ee-cartassol'  #  'geo-data-s' # 'mapbiomas-caatinga-cloud02'
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

#exporta a imagem classificada para o asset
def processoExportar(mapaRF,  nomeDesc, pathIC):    
    # pathIC = "projects/geo-data-s/assets/fotovoltaica/version_2"
    idasset =  os.path.join(pathIC, nomeDesc)
    optExp = {
        'image': mapaRF, 
        'description': nomeDesc, 
        'assetId':idasset, 
        'region': mapaRF.geometry(), #.getInfo()['coordinates'],
        'scale': 10, 
        'crs': "EPSG:3857",
        'maxPixels': 1e13,
        "pyramidingPolicy":{".default": "mode"}
    }
    task = ee.batch.Export.image.toAsset(**optExp)
    task.start() 
    print("salvando ... " + nomeDesc + "..!")



createFolder = False
createIC = False
version = 1
mes = 10
pathsIC = "projects/mapbiomas-workspace/AMOSTRAS/col10/embedding_tessera"
# ******************** Command Line Instructions ******************************#
# Here we create a folder in GEE using earthengine tool
# references https://cloud.google.com/sdk/gcloud/reference/storage
if createFolder:
    folderDir = 'projects/geo-data-s/assets/fotovoltaica'
    comando = "earthengine create folder " + folderDir
    os.system(comando)
    print(f" folder in asset < {comando} > create !" )
# Create a collection (it looks like another folder, but internally it will be
# an image collection).
if createIC:    
    comando = "earthengine create collection " + pathsIC
    os.system(comando)
    print(f" image Collection < {comando} > create !" )


img_col = ee.ImageCollection(pathsIC)
lstCod  = img_col.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list').getInfo()
print("size of imagem ", len(lstCod));
print(lstCod[: 5])
lstCod_id = [kk.replace(".", "") for kk in lstCod]
# sys.exit()


# ******************** Bash script ********************************************#

gcBucket= "mapbiomas-energia"
folderDirGS = 'embedding_tif/'

comando = f"gcloud storage ls gs://{gcBucket}/{folderDirGS}*"
lstdirsFileGS = os.system(comando)  # os.system
print(lstdirsFileGS)
processo = subprocess.check_output(comando, shell=True)
# tmp = processo.read()
# print(processo)
# print(type(processo))
lstdirsFileGS = str(processo.decode('utf-8')).split("\n")
nyear = 2024

data_inic = ee.Date.fromYMD(nyear,1, 1)
dictProp = {        
    'year': nyear,
    'version': str(version), 
    'data_inic':  data_inic, 
    'data_end':  data_inic.advance(1,'year'), 
    'system:time_start': data_inic
}
vdate = data_inic.format('YYYY-MM-DD').getInfo()
# print(vdate)
# sys.exit()
# Get file names to extract date and call ingestion command for each file to be added into an asset as image collection
for cc, pathdir in enumerate(lstdirsFileGS[:]):
    namefile = pathdir.split("/")[-1]
    print(namefile)
    if "grid_" in namefile and f"_{nyear}" in namefile:  # 
        if namefile not in lstCod_id:
            print(f" #{cc} >> {namefile}")            
            idAsset = os.path.join(pathsIC, namefile)
            print("idAsset >> ", idAsset)
            # grid_-47.45_-23.95_SF-23-Y-C-IV_2024_g2d.tif
            partes = namefile.split("_")
            # https://developers.google.com/earth-engine/guides/command_line
            # earthengine upload image --asset_id=users/myuser/asset --pyramiding_policy=sample --nodata_value=255 gs://bucket/image.tif
            # newComand = f"earthengine upload image --asset_id={idAsset} --time_start={vdate} --pyramiding_policy=mode  --nodata_value=-9999 {pathdir}"
            # #  --property={dictProp}
            # os.system(newComand)
            # print(" processing ... ")
            imgTIF = ee.Image.loadGeoTIFF(pathdir)
            # imgTIF = ee.Image.fromCloudTiff(pathdir)
            imgTIF = ee.Image(imgTIF).set(
                    'year', nyear, 'source', 'geotessera',
                    'version', str(version), 'data_inic',  data_inic, 
                    'longitud',  partes[1], 'latitud', partes[2],  
                    'carta',  partes[3],       
                    'system:time_start', data_inic)
            if cc > -1:
                processoExportar(imgTIF, namefile.replace(".", "_"), pathsIC)
        else:
            print(f" #{cc} >> tile já feito >>>  {namefile}")   
