from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.postgres_client import PostgresClient


class SilverToGoldIngestion:
    """
    Transform silver parquet datasets into gold star-schema tables
    and load them into PostgreSQL.
    """

    def __init__(self, silver_dir: str) -> None:
        load_dotenv()
        self.silver_dir = Path(silver_dir)
        self.client = PostgresClient()

        self.domain_folder_map = {
            "acidentes": "Acidentes_DadosAbertos",
            "localidade": "Localidade_DadosAbertos",
            "tipo_veiculo": "TipoVeiculo_DadosAbertos",
            "vitimas": "Vitimas_DadosAbertos",
        }

        self.load_order = [
            "dim_tempo",
            "dim_localidade",
            "dim_condicao_via",
            "dim_vitima",
            "dim_tipo_veiculo",
            "fato_acidente",
            "fato_vitima",
            "fato_veiculo_acidente",
        ]

    def _read_domain_parts(self, folder_name: str) -> pd.DataFrame:
        domain_path = self.silver_dir / folder_name

        if not domain_path.exists():
            raise FileNotFoundError(f"Silver folder not found: {domain_path}")

        parquet_parts = sorted(domain_path.glob("part_*.parquet"))

        if not parquet_parts:
            raise FileNotFoundError(f"No parquet parts found in: {domain_path}")

        dataframes: List[pd.DataFrame] = []

        for parquet_file in parquet_parts:
            print(f"Reading silver part: {parquet_file}")
            dataframes.append(pd.read_parquet(parquet_file))

        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        print(
            f"Combined {len(parquet_parts)} parquet part(s) from {folder_name}: "
            f"{len(combined_dataframe)} row(s)"
        )
        return combined_dataframe

    def read_silver_domains(self) -> Dict[str, pd.DataFrame]:
        datasets: Dict[str, pd.DataFrame] = {}

        for domain_name, folder_name in self.domain_folder_map.items():
            datasets[domain_name] = self._read_domain_parts(folder_name)

        return datasets

    def build_dim_tempo(self, acidentes_df: pd.DataFrame) -> pd.DataFrame:
        df = acidentes_df.copy()

        df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")

        df["ano"] = df["data_acidente"].dt.year
        df["mes"] = df["data_acidente"].dt.month

        df["mes_ano"] = df["ano"] * 100 + df["mes"]

        df["dia_semana"] = df["data_acidente"].dt.day_name()

        dim_tempo = (
            df[["data_acidente", "ano", "mes", "mes_ano", "dia_semana"]]
            .drop_duplicates()
            .sort_values("data_acidente")
            .reset_index(drop=True)
        )

        dim_tempo["pk_tempo"] = dim_tempo.index + 1

        dim_tempo = dim_tempo.rename(columns={"data_acidente": "data"})

        dim_tempo = dim_tempo[
            ["pk_tempo", "data", "ano", "mes", "mes_ano", "dia_semana"]
        ]

        return dim_tempo

    def build_dim_localidade(self, localidade_df: pd.DataFrame) -> pd.DataFrame:
        df = localidade_df.copy()

        dim_localidade = df[
            [
                "chv_localidade",
                "ano_referencia",
                "mes_referencia",
                "regiao",
                "uf",
                "codigo_ibge",
                "municipio",
                "regiao_metropolitana",
                "qtde_habitantes",
                "frota_total",
                "frota_circulante",
            ]
        ].drop_duplicates()

        dim_localidade = dim_localidade.rename(
            columns={
                "chv_localidade": "localidade",
                "ano_referencia": "ano_ref",
                "mes_referencia": "mes_ref",
                "codigo_ibge": "cod_ibge",
            }
        )

        dim_localidade = dim_localidade.sort_values(
            ["localidade", "ano_ref", "mes_ref"]
        ).reset_index(drop=True)

        dim_localidade["pk_localidade"] = dim_localidade.index + 1

        dim_localidade = dim_localidade[
            [
                "pk_localidade",
                "localidade",
                "ano_ref",
                "mes_ref",
                "regiao",
                "uf",
                "cod_ibge",
                "municipio",
                "regiao_metropolitana",
                "qtde_habitantes",
                "frota_total",
                "frota_circulante",
            ]
        ]

        return dim_localidade

    def build_dim_condicao_via(self, acidentes_df: pd.DataFrame) -> pd.DataFrame:
        df = acidentes_df.copy()

        dim_condicao_via = df[
            [
                "fase_dia",
                "lim_velocidade",
                "tp_pista",
                "ind_guardrail",
                "ind_cantcentral",
                "ind_acostamento",
            ]
        ].drop_duplicates()

        dim_condicao_via = dim_condicao_via.sort_values(
            [
                "fase_dia",
                "lim_velocidade",
                "tp_pista",
                "ind_guardrail",
                "ind_cantcentral",
                "ind_acostamento",
            ]
        ).reset_index(drop=True)

        dim_condicao_via["pk_condicao_via"] = dim_condicao_via.index + 1

        dim_condicao_via = dim_condicao_via[
            [
                "pk_condicao_via",
                "fase_dia",
                "lim_velocidade",
                "tp_pista",
                "ind_guardrail",
                "ind_cantcentral",
                "ind_acostamento",
            ]
        ]

        return dim_condicao_via

    def build_dim_vitima(self, vitimas_df: pd.DataFrame) -> pd.DataFrame:
        df = vitimas_df.copy()

        dim_vitima = df[
            [
                "faixa_idade",
                "genero",
                "tp_envolvido",
                "gravidade_lesao",
                "equip_seguranca",
                "ind_motorista",
                "susp_alcool",
            ]
        ].drop_duplicates()

        dim_vitima = dim_vitima.sort_values(
            [
                "faixa_idade",
                "genero",
                "tp_envolvido",
                "gravidade_lesao",
                "equip_seguranca",
                "ind_motorista",
                "susp_alcool",
            ]
        ).reset_index(drop=True)

        dim_vitima["pk_vitima"] = dim_vitima.index + 1

        dim_vitima = dim_vitima[
            [
                "pk_vitima",
                "faixa_idade",
                "genero",
                "tp_envolvido",
                "gravidade_lesao",
                "equip_seguranca",
                "ind_motorista",
                "susp_alcool",
            ]
        ]

        return dim_vitima

    def build_dim_tipo_veiculo(self, tipo_veiculo_df: pd.DataFrame) -> pd.DataFrame:
        df = tipo_veiculo_df.copy()

        dim_tipo_veiculo = df[
            [
                "tipo_veiculo",
                "ind_veic_estrangeiro",
            ]
        ].drop_duplicates()

        dim_tipo_veiculo = dim_tipo_veiculo.sort_values(
            [
                "tipo_veiculo",
                "ind_veic_estrangeiro",
            ]
        ).reset_index(drop=True)

        dim_tipo_veiculo["pk_tipo_veiculo"] = dim_tipo_veiculo.index + 1

        dim_tipo_veiculo = dim_tipo_veiculo[
            [
                "pk_tipo_veiculo",
                "tipo_veiculo",
                "ind_veic_estrangeiro",
            ]
        ]

        return dim_tipo_veiculo

    def build_fato_acidente(
        self,
        acidentes_df: pd.DataFrame,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_condicao_via: pd.DataFrame,
    ) -> pd.DataFrame:
        df = acidentes_df.copy()

        df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")

        fato_acidente = df.merge(
            dim_tempo[["pk_tempo", "data"]],
            left_on="data_acidente",
            right_on="data",
            how="left",
        )

        fato_acidente = fato_acidente.merge(
            dim_localidade[["pk_localidade", "localidade"]],
            left_on="chv_localidade",
            right_on="localidade",
            how="left",
        )

        fato_acidente = fato_acidente.merge(
            dim_condicao_via[
                [
                    "pk_condicao_via",
                    "fase_dia",
                    "lim_velocidade",
                    "tp_pista",
                    "ind_guardrail",
                    "ind_cantcentral",
                    "ind_acostamento",
                ]
            ],
            on=[
                "fase_dia",
                "lim_velocidade",
                "tp_pista",
                "ind_guardrail",
                "ind_cantcentral",
                "ind_acostamento",
            ],
            how="left",
        )

        fato_acidente = fato_acidente[
            [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_condicao_via",
                "qtde_acidente",
                "qtde_acid_com_obitos",
                "qtde_envolvidos",
                "qtde_feridos_ilesos",
                "qtde_obitos",
            ]
        ].copy()

        numeric_columns = [
            "num_acidente",
            "pk_tempo",
            "pk_localidade",
            "pk_condicao_via",
            "qtde_acidente",
            "qtde_acid_com_obitos",
            "qtde_envolvidos",
            "qtde_feridos_ilesos",
            "qtde_obitos",
        ]

        for column in numeric_columns:
            fato_acidente[column] = pd.to_numeric(fato_acidente[column], errors="coerce")

        fato_acidente = fato_acidente.reset_index(drop=True)
        fato_acidente["pk_fato_acidente"] = fato_acidente.index + 1

        fato_acidente = fato_acidente[
            [
                "pk_fato_acidente",
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_condicao_via",
                "qtde_acidente",
                "qtde_acid_com_obitos",
                "qtde_envolvidos",
                "qtde_feridos_ilesos",
                "qtde_obitos",
            ]
        ]

        return fato_acidente

    def build_fato_vitima(
        self,
        vitimas_df: pd.DataFrame,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_vitima: pd.DataFrame,
    ) -> pd.DataFrame:
        df = vitimas_df.copy()

        df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")

        df["qtde_envolvidos"] = pd.to_numeric(df["qtde_envolvidos"], errors="coerce")
        df["qtde_feridos_ilesos"] = pd.to_numeric(df["qtde_feridos_ilesos"], errors="coerce")
        df["qtde_obitos"] = pd.to_numeric(df["qtde_obitos"], errors="coerce")

        df["flag_obito"] = df["qtde_obitos"] > 0
        df["flag_ferido"] = (
            (df["qtde_envolvidos"] > 1)
            & (df["qtde_feridos_ilesos"] < df["qtde_envolvidos"])
        )

        fato_vitima = df.merge(
            dim_tempo[["pk_tempo", "data"]],
            left_on="data_acidente",
            right_on="data",
            how="left",
        )

        fato_vitima = fato_vitima.merge(
            dim_localidade[["pk_localidade", "localidade"]],
            left_on="chv_localidade",
            right_on="localidade",
            how="left",
        )

        fato_vitima = fato_vitima.merge(
            dim_vitima[
                [
                    "pk_vitima",
                    "faixa_idade",
                    "genero",
                    "tp_envolvido",
                    "gravidade_lesao",
                    "equip_seguranca",
                    "ind_motorista",
                    "susp_alcool",
                ]
            ],
            on=[
                "faixa_idade",
                "genero",
                "tp_envolvido",
                "gravidade_lesao",
                "equip_seguranca",
                "ind_motorista",
                "susp_alcool",
            ],
            how="left",
        )

        fato_vitima = fato_vitima[
            [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_vitima",
                "flag_obito",
                "flag_ferido",
            ]
        ].copy()

        numeric_columns = [
            "num_acidente",
            "pk_tempo",
            "pk_localidade",
            "pk_vitima",
        ]

        for column in numeric_columns:
            fato_vitima[column] = pd.to_numeric(fato_vitima[column], errors="coerce")

        fato_vitima = fato_vitima.reset_index(drop=True)
        fato_vitima["pk_fato_vitima"] = fato_vitima.index + 1

        fato_vitima = fato_vitima[
            [
                "pk_fato_vitima",
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_vitima",
                "flag_obito",
                "flag_ferido",
            ]
        ]

        return fato_vitima

    def build_fato_veiculo_acidente(
        self,
        tipo_veiculo_df: pd.DataFrame,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_tipo_veiculo: pd.DataFrame,
    ) -> pd.DataFrame:
        df = tipo_veiculo_df.copy()

        df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")

        df["qtde_veiculos"] = pd.to_numeric(df["qtde_veiculos"], errors="coerce")

        fato_veiculo_acidente = df.merge(
            dim_tempo[["pk_tempo", "data"]],
            left_on="data_acidente",
            right_on="data",
            how="left",
        )

        fato_veiculo_acidente = fato_veiculo_acidente.merge(
            dim_localidade[["pk_localidade", "localidade"]],
            left_on="chv_localidade",
            right_on="localidade",
            how="left",
        )

        fato_veiculo_acidente = fato_veiculo_acidente.merge(
            dim_tipo_veiculo[
                [
                    "pk_tipo_veiculo",
                    "tipo_veiculo",
                    "ind_veic_estrangeiro",
                ]
            ],
            on=[
                "tipo_veiculo",
                "ind_veic_estrangeiro",
            ],
            how="left",
        )

        fato_veiculo_acidente = fato_veiculo_acidente[
            [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_tipo_veiculo",
                "qtde_veiculos",
            ]
        ].copy()

        numeric_columns = [
            "num_acidente",
            "pk_tempo",
            "pk_localidade",
            "pk_tipo_veiculo",
            "qtde_veiculos",
        ]

        for column in numeric_columns:
            fato_veiculo_acidente[column] = pd.to_numeric(
                fato_veiculo_acidente[column],
                errors="coerce",
            )

        fato_veiculo_acidente = fato_veiculo_acidente.reset_index(drop=True)
        fato_veiculo_acidente["pk_fato_veiculo"] = fato_veiculo_acidente.index + 1

        fato_veiculo_acidente = fato_veiculo_acidente[
            [
                "pk_fato_veiculo",
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_tipo_veiculo",
                "qtde_veiculos",
            ]
        ]

        return fato_veiculo_acidente

    def build_gold_tables(self) -> Dict[str, pd.DataFrame]:
        silver_datasets = self.read_silver_domains()

        dim_tempo = self.build_dim_tempo(silver_datasets["acidentes"])
        dim_localidade = self.build_dim_localidade(silver_datasets["localidade"])
        dim_condicao_via = self.build_dim_condicao_via(silver_datasets["acidentes"])
        dim_vitima = self.build_dim_vitima(silver_datasets["vitimas"])
        dim_tipo_veiculo = self.build_dim_tipo_veiculo(silver_datasets["tipo_veiculo"])

        fato_acidente = self.build_fato_acidente(
            acidentes_df=silver_datasets["acidentes"],
            dim_tempo=dim_tempo,
            dim_localidade=dim_localidade,
            dim_condicao_via=dim_condicao_via,
        )

        fato_vitima = self.build_fato_vitima(
            vitimas_df=silver_datasets["vitimas"],
            dim_tempo=dim_tempo,
            dim_localidade=dim_localidade,
            dim_vitima=dim_vitima,
        )

        fato_veiculo_acidente = self.build_fato_veiculo_acidente(
            tipo_veiculo_df=silver_datasets["tipo_veiculo"],
            dim_tempo=dim_tempo,
            dim_localidade=dim_localidade,
            dim_tipo_veiculo=dim_tipo_veiculo,
        )

        return {
            "dim_tempo": dim_tempo,
            "dim_localidade": dim_localidade,
            "dim_condicao_via": dim_condicao_via,
            "dim_vitima": dim_vitima,
            "dim_tipo_veiculo": dim_tipo_veiculo,
            "fato_acidente": fato_acidente,
            "fato_vitima": fato_vitima,
            "fato_veiculo_acidente": fato_veiculo_acidente,
        }

    def truncate_gold_tables(self) -> None:
        reverse_load_order = list(reversed(self.load_order))

        for table_name in reverse_load_order:
            sql = f"TRUNCATE TABLE {self.client.schema}.{table_name} RESTART IDENTITY CASCADE;"
            print(f"Truncating table: {table_name}")
            self.client.execute_sql(sql)

    def load_dataframe(self, table_name: str, dataframe: pd.DataFrame) -> None:
        print(f"Loading table {table_name}: {len(dataframe)} row(s)")

        dataframe.to_sql(
            name=table_name,
            con=self.client.engine,
            schema=self.client.schema,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )

        print(f"Table loaded successfully: {table_name}")

    def validate_table(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        sql = text(
            f"SELECT * FROM {self.client.schema}.{table_name} LIMIT :limit"
        )

        with self.client.engine.connect() as connection:
            dataframe = pd.read_sql(sql, connection, params={"limit": limit})

        return dataframe

    def run(self, truncate_before_load: bool = True) -> Dict[str, pd.DataFrame]:
        self.client.test_connection()
        gold_tables = self.build_gold_tables()

        if truncate_before_load:
            self.truncate_gold_tables()

        for table_name in self.load_order:
            self.load_dataframe(table_name, gold_tables[table_name])

        print("All gold tables were loaded successfully into PostgreSQL.")
        return gold_tables


if __name__ == "__main__":
    loader = SilverToGoldIngestion(silver_dir="data/silver")
    loader.run(truncate_before_load=True)
