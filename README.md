# download_embedding_tessera
este é um scripts para fazer download do dado de tessera de embedding


passo # 1 (installar biblioteca)
    Rodar no terminal 
    A - pip install geotessera
    # Generate a coverage map for the BR
    B - geotessera coverage --country BR

passo #2 
    A - cd download_embedding_tessera/src/process
    B - python make_download_tessera.py

passo #3 
    A - cd download_embedding_tessera/src/posprocssing
    B - python uploadTIF_from_localFolder_GoogleStorage.py
    B - python uploadTIF_from_localFolder_GoogleStorage_research.py


Scripts e, Jupyter com Analises e seu jupyter
tentativas de baixar por box usanndo o git https://github.com/ucam-eo/geotessera
https://colab.research.google.com/drive/1or85gO1cCEqptaXhn9VGPVAuOduqePPS?usp=sharing