# Lab01 – Pipeline de Engenharia de Dados (NUSP: 97103)

## 1. Configuração do Ambiente

Repositório:

[https://github.com/dianedeps/Lab01_PART1_97103](https://github.com/dianedeps/Lab01_PART1_97103)

### Clonagem

```bash
git clone https://github.com/dianedeps/Lab01_PART1_97103
cd Lab01_PART1_97103
```

### Ambiente Python

```bash
conda create -n lab01 python=3.10
conda activate lab01
pip install -r requirements.txt
```

### Configuração do Kaggle

```bash
export KAGGLE_API_TOKEN=SEU_TOKEN
```

---

## 2. Tecnologias Utilizadas

* Python

  * pandas
  * numpy
  * matplotlib
  * seaborn
  * pyarrow
* PostgreSQL
* DBeaver
* Conda
* Kaggle API

---

## 3. Fonte de Dados

Dataset utilizado:

* Fonte: Kaggle
* Link: [https://www.kaggle.com/datasets/mos3santos/quantitativo-de-acidentes-de-trnsito-no-brasil/data](https://www.kaggle.com/datasets/mos3santos/quantitativo-de-acidentes-de-trnsito-no-brasil/data)

Características do dataset:

* 32 arquivos CSV
* 544 colunas
* Tipos variados (String, Integer, DateTime)

---

## 4. Arquitetura da Solução

O pipeline foi estruturado em três camadas:

* Bronze (Raw)
* Silver (Tratamento)
* Gold (Modelagem)

### Fluxo do Pipeline

```text
[Kaggle API]
      ↓
Download dos dados (Python)
      ↓
Armazenamento em data/raw
      ↓
Profiling dos dados raw
      ↓
Ingestão Raw → Silver (CSV → Parquet + tratamento)
      ↓
Ingestão Silver → Gold (modelagem + carga)
      ↓
PostgreSQL (tabelas analíticas)
```

---

## 5. Execução do Projeto

Ordem de execução:

1. 01_kaggle_download.ipynb
2. 02_raw_to_silver_ingestion.ipynb
3. 03_raw_data_profiling.ipynb
4. 04_silver_to_gold.ipynb

---

## 6. Camada Bronze (Raw)

Armazena os dados brutos sem transformação.

Processos:

* Download via Kaggle
* Armazenamento em `data/raw/`
* Leitura como string
* Inclusão de colunas de controle:

  * source_file
  * ingestion_date

---

## 7. Camada Silver (Tratamento)

Responsável pela preparação dos dados.

Etapas:

* Profiling:

  * Nulos
  * Tipos
  * Estatísticas

* Limpeza:

  * Remoção de duplicados
  * Tratamento de nulos
  * Conversão de tipos

* Padronização:

  * snake_case

* Armazenamento:

  * Parquet em `data/silver/`

---

## 8. Camada Gold (Modelagem)

Modelagem dimensional para análise.

Modelo:

* Fato:

  * fato_acidente

* Dimensões:

  * dim_tempo
  * dim_localidade
  * dim_condicao_via
  * dim_vitima
  * dim_tipo_veiculo

Banco:

* PostgreSQL
* Database: lab01

Processo:

* Leitura da Silver
* Transformação para modelo dimensional
* Carga no PostgreSQL

---

## 9. Entregáveis

* Ingestão de dados
* Camada Bronze
* Camada Silver
* Profiling e visualizações
* Conversão para Parquet
* Modelagem Star Schema
* Carga no PostgreSQL

---
