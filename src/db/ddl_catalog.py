DDL_CATALOG = {

    "dim_tempo": """
        CREATE TABLE IF NOT EXISTS public.dim_tempo (
            pk_tempo INTEGER PRIMARY KEY,
            data DATE,
            ano INTEGER,
            mes INTEGER,
            mes_ano INTEGER,
            dia_semana VARCHAR(20)
        );
    """,

    "dim_localidade": """
        CREATE TABLE IF NOT EXISTS public.dim_localidade (
            pk_localidade INTEGER PRIMARY KEY,
            localidade VARCHAR(100),
            ano_ref INTEGER,
            mes_ref INTEGER,
            regiao VARCHAR(50),
            uf VARCHAR(2),
            cod_ibge INTEGER,
            municipio VARCHAR(100),
            regiao_metropolitana VARCHAR(20),
            qtde_habitantes BIGINT,
            frota_total BIGINT,
            frota_circulante BIGINT
        );
    """,

    "dim_condicao_via": """
        CREATE TABLE IF NOT EXISTS public.dim_condicao_via (
            pk_condicao_via INTEGER PRIMARY KEY,
            fase_dia VARCHAR(50),
            lim_velocidade VARCHAR(30),
            tp_pista VARCHAR(50),
            ind_guardrail VARCHAR(20),
            ind_cantcentral VARCHAR(20),
            ind_acostamento VARCHAR(20)
        );
    """,

    "dim_vitima": """
        CREATE TABLE IF NOT EXISTS public.dim_vitima (
            pk_vitima INTEGER PRIMARY KEY,
            faixa_idade VARCHAR(50),
            genero VARCHAR(20),
            tp_envolvido VARCHAR(50),
            gravidade_lesao VARCHAR(50),
            equip_seguranca VARCHAR(100),
            ind_motorista VARCHAR(20),
            susp_alcool VARCHAR(20)
        );
    """,

    "dim_tipo_veiculo": """
        CREATE TABLE IF NOT EXISTS public.dim_tipo_veiculo (
            pk_tipo_veiculo INTEGER PRIMARY KEY,
            tipo_veiculo VARCHAR(100),
            ind_veic_estrangeiro VARCHAR(20)
        );
    """,

    "fato_acidente": """
        CREATE TABLE IF NOT EXISTS public.fato_acidente (
            pk_fato_acidente BIGINT PRIMARY KEY,
            num_acidente BIGINT,
            pk_tempo INTEGER,
            pk_localidade INTEGER,
            pk_condicao_via INTEGER,
            qtde_acidente INTEGER,
            qtde_acid_com_obitos INTEGER,
            qtde_envolvidos INTEGER,
            qtde_feridosilesos INTEGER,
            qtde_obitos INTEGER,
            FOREIGN KEY (pk_tempo) REFERENCES public.dim_tempo(pk_tempo),
            FOREIGN KEY (pk_localidade) REFERENCES public.dim_localidade(pk_localidade),
            FOREIGN KEY (pk_condicao_via) REFERENCES public.dim_condicao_via(pk_condicao_via)
        );
    """,

    "fato_vitima": """
        CREATE TABLE IF NOT EXISTS public.fato_vitima (
            pk_fato_vitima BIGINT PRIMARY KEY,
            num_acidente BIGINT,
            pk_tempo INTEGER,
            pk_localidade INTEGER,
            pk_vitima INTEGER,
            flag_obito BOOLEAN,
            flag_ferido BOOLEAN,
            FOREIGN KEY (pk_tempo) REFERENCES public.dim_tempo(pk_tempo),
            FOREIGN KEY (pk_localidade) REFERENCES public.dim_localidade(pk_localidade),
            FOREIGN KEY (pk_vitima) REFERENCES public.dim_vitima(pk_vitima)
        );
    """,

    "fato_veiculo_acidente": """
        CREATE TABLE IF NOT EXISTS public.fato_veiculo_acidente (
            pk_fato_veiculo BIGINT PRIMARY KEY,
            num_acidente BIGINT,
            pk_tempo INTEGER,
            pk_localidade INTEGER,
            pk_tipo_veiculo INTEGER,
            qtde_veiculos INTEGER,
            FOREIGN KEY (pk_tempo) REFERENCES public.dim_tempo(pk_tempo),
            FOREIGN KEY (pk_localidade) REFERENCES public.dim_localidade(pk_localidade),
            FOREIGN KEY (pk_tipo_veiculo) REFERENCES public.dim_tipo_veiculo(pk_tipo_veiculo)
        );
    """,
}