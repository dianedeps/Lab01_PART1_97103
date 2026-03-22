## Lab01 Part1 - Num. USP: 97103

#### 1. Setup & Infra

Criar repositório no GitHub: https://github.com/dianedeps/Lab01_PART1_97103
Configurar ambiente Python (virtualizado com Conda)


#### 2. Ativação local

No Git Bash: git checkout branch feature/setup-env
No Anaconda Prompt: conda activate lab01
No VSCode: Vincular ambiente Ctrl + Shift + P e selecionar o lab01


#### 3. Instalar PostgreSQL + ferramenta (DBeaver)

https://www.postgresql.org/download/
No SQL Shell: postgres up and running
Criei um DB: lab01

https://dbeaver.io/download/
Dbeaver conectado ao Postgres


#### 4. Data Source

Opção 1: kagglehub: Python library
    https://www.kaggle.com/docs/api
    https://github.com/Kaggle/kagglehub

    API Token:
        KGAT_meutoken
    
    To use this token, set the KAGGLE_API_TOKEN environment variable:
        export KAGGLE_API_TOKEN=KGAT_meutoken
    
    After setting KAGGLE_API_TOKEN, you can use the client as follows:
        kaggle competitions list

Opção 2: kaggle old style (.json file com user e token)
    https://github.com/Kaggle/kaggle-cli


Dataset escolhido:
    https://www.kaggle.com/datasets/mos3santos/quantitativo-de-acidentes-de-trnsito-no-brasil/data

    Summary
        32 files
            .csv
        544 columns
            String 272
            Integer 240
            DateTime 16
            Other 16


#### 5. Bronze (Raw)

Criar script de ingestão para lz
Criar script de ingestão para raw, salvar dados “as-is” em data/raw/
