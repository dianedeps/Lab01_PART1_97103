from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.postgres_client import PostgresClient


class SilverToGoldIngestion:
    """
    Read silver parquet parts incrementally, build gold dimensions/facts,
    and load them into PostgreSQL using COPY for better local performance.
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

    def _list_domain_parts(self, folder_name: str) -> List[Path]:
        domain_path = self.silver_dir / folder_name

        if not domain_path.exists():
            raise FileNotFoundError(f"Silver folder not found: {domain_path}")

        parquet_parts = sorted(domain_path.glob("part_*.parquet"))

        if not parquet_parts:
            raise FileNotFoundError(f"No parquet parts found in: {domain_path}")

        return parquet_parts

    def _read_parquet_part(
        self,
        parquet_file: Path,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        print(f"Reading silver part: {parquet_file}")
        return pd.read_parquet(parquet_file, columns=columns)

    def truncate_gold_tables(self) -> None:
        reverse_load_order = list(reversed(self.load_order))

        for table_name in reverse_load_order:
            sql = (
                f"TRUNCATE TABLE {self.client.schema}.{table_name} "
                f"RESTART IDENTITY CASCADE;"
            )
            print(f"Truncating table: {table_name}")
            self.client.execute_sql(sql)

    def validate_table(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        sql = text(f"SELECT * FROM {self.client.schema}.{table_name} LIMIT :limit")

        with self.client.engine.connect() as connection:
            dataframe = pd.read_sql(sql, connection, params={"limit": limit})

        return dataframe

    def _validate_null_keys(
        self,
        dataframe: pd.DataFrame,
        key_columns: list[str],
        table_name: str,
    ) -> None:
        null_counts = dataframe[key_columns].isna().sum()
        invalid = null_counts[null_counts > 0]

        if not invalid.empty:
            raise ValueError(
                f"Null foreign keys detected in {table_name}: {invalid.to_dict()}"
            )

    def _normalize_dataframe_for_copy(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy()

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        df = df.where(pd.notnull(df), None)

        for col in df.columns:
            if pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].map(
                    lambda value: None if value is None else bool(value)
                )

        return df

    def load_dataframe(self, table_name: str, dataframe: pd.DataFrame) -> None:
        if dataframe.empty:
            print(f"Skipping empty dataframe for table: {table_name}")
            return

        print(f"Loading table {table_name}: {len(dataframe)} row(s)")

        normalized_df = self._normalize_dataframe_for_copy(dataframe)

        buffer = io.StringIO()
        normalized_df.to_csv(
            buffer,
            index=False,
            header=False,
            sep=",",
            na_rep="",
            quoting=csv.QUOTE_MINIMAL,
        )
        buffer.seek(0)

        raw_connection = self.client.engine.raw_connection()

        try:
            with raw_connection.cursor() as cursor:
                columns_csv = ", ".join(normalized_df.columns)
                copy_sql = (
                    f"COPY {self.client.schema}.{table_name} ({columns_csv}) "
                    f"FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')"
                )
                cursor.copy_expert(copy_sql, buffer)

            raw_connection.commit()
            print(f"Table loaded successfully: {table_name}")

        except Exception:
            raw_connection.rollback()
            raise

        finally:
            raw_connection.close()

    def build_dim_tempo(self) -> pd.DataFrame:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["acidentes"])
        frames: list[pd.DataFrame] = []

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=["data_acidente"])
            df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")
            df["ano"] = df["data_acidente"].dt.year
            df["mes"] = df["data_acidente"].dt.month
            df["mes_ano"] = df["ano"] * 100 + df["mes"]
            df["dia_semana"] = df["data_acidente"].dt.day_name()

            frames.append(
                df[["data_acidente", "ano", "mes", "mes_ano", "dia_semana"]]
            )

        dim_tempo = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .sort_values("data_acidente")
            .reset_index(drop=True)
        )

        dim_tempo["pk_tempo"] = dim_tempo.index + 1
        dim_tempo = dim_tempo.rename(columns={"data_acidente": "data"})

        return dim_tempo[
            ["pk_tempo", "data", "ano", "mes", "mes_ano", "dia_semana"]
        ]

    def build_dim_localidade(self) -> pd.DataFrame:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["localidade"])
        frames: list[pd.DataFrame] = []

        columns = [
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

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)

            for col in [
                "ano_referencia",
                "mes_referencia",
                "codigo_ibge",
                "qtde_habitantes",
                "frota_total",
                "frota_circulante",
            ]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            frames.append(df)

        dim_localidade = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .rename(
                columns={
                    "chv_localidade": "localidade",
                    "ano_referencia": "ano_ref",
                    "mes_referencia": "mes_ref",
                    "codigo_ibge": "cod_ibge",
                }
            )
            .sort_values(["localidade", "ano_ref", "mes_ref"])
            .reset_index(drop=True)
        )

        dim_localidade["pk_localidade"] = dim_localidade.index + 1

        return dim_localidade[
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

    def build_dim_condicao_via(self) -> pd.DataFrame:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["acidentes"])
        frames: list[pd.DataFrame] = []

        columns = [
            "fase_dia",
            "lim_velocidade",
            "tp_pista",
            "ind_guardrail",
            "ind_cantcentral",
            "ind_acostamento",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            frames.append(df)

        dim_condicao_via = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .sort_values(columns)
            .reset_index(drop=True)
        )

        dim_condicao_via["pk_condicao_via"] = dim_condicao_via.index + 1

        return dim_condicao_via[
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

    def build_dim_vitima(self) -> pd.DataFrame:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["vitimas"])
        frames: list[pd.DataFrame] = []

        columns = [
            "faixa_idade",
            "genero",
            "tp_envolvido",
            "gravidade_lesao",
            "equip_seguranca",
            "ind_motorista",
            "susp_alcool",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            frames.append(df)

        dim_vitima = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .sort_values(columns)
            .reset_index(drop=True)
        )

        dim_vitima["pk_vitima"] = dim_vitima.index + 1

        return dim_vitima[
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

    def build_dim_tipo_veiculo(self) -> pd.DataFrame:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["tipo_veiculo"])
        frames: list[pd.DataFrame] = []

        columns = [
            "tipo_veiculo",
            "ind_veic_estrangeiro",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            frames.append(df)

        dim_tipo_veiculo = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .sort_values(columns)
            .reset_index(drop=True)
        )

        dim_tipo_veiculo["pk_tipo_veiculo"] = dim_tipo_veiculo.index + 1

        return dim_tipo_veiculo[
            [
                "pk_tipo_veiculo",
                "tipo_veiculo",
                "ind_veic_estrangeiro",
            ]
        ]

    def build_acidente_lookup(self) -> pd.DataFrame:
        """
        Build a lightweight lookup from acidentes to resolve pk_tempo and pk_localidade
        for facts that only carry num_acidente in their own domain.
        """
        parquet_parts = self._list_domain_parts(self.domain_folder_map["acidentes"])
        frames: list[pd.DataFrame] = []

        columns = [
            "num_acidente",
            "data_acidente",
            "chv_localidade",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")
            frames.append(df)

        acidente_lookup = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["num_acidente"])
            .reset_index(drop=True)
        )

        return acidente_lookup

    def load_fato_acidente(
        self,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_condicao_via: pd.DataFrame,
    ) -> None:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["acidentes"])
        next_pk = 1

        columns = [
            "num_acidente",
            "data_acidente",
            "chv_localidade",
            "fase_dia",
            "lim_velocidade",
            "tp_pista",
            "ind_guardrail",
            "ind_cantcentral",
            "ind_acostamento",
            "qtde_acidente",
            "qtde_acid_com_obitos",
            "qtde_envolvidos",
            "qtde_feridosilesos",
            "qtde_obitos",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")

            fato = df.merge(
                dim_tempo[["pk_tempo", "data"]],
                left_on="data_acidente",
                right_on="data",
                how="left",
            )

            fato = fato.merge(
                dim_localidade[["pk_localidade", "localidade"]],
                left_on="chv_localidade",
                right_on="localidade",
                how="left",
            )

            fato = fato.merge(
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

            fato = fato[
                [
                    "num_acidente",
                    "pk_tempo",
                    "pk_localidade",
                    "pk_condicao_via",
                    "qtde_acidente",
                    "qtde_acid_com_obitos",
                    "qtde_envolvidos",
                    "qtde_feridosilesos",
                    "qtde_obitos",
                ]
            ].copy()

            for col in [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_condicao_via",
                "qtde_acidente",
                "qtde_acid_com_obitos",
                "qtde_envolvidos",
                "qtde_feridosilesos",
                "qtde_obitos",
            ]:
                fato[col] = pd.to_numeric(fato[col], errors="coerce")

            self._validate_null_keys(
                fato,
                ["pk_tempo", "pk_localidade", "pk_condicao_via"],
                "fato_acidente",
            )

            fato = fato.reset_index(drop=True)
            fato["pk_fato_acidente"] = range(next_pk, next_pk + len(fato))
            next_pk += len(fato)

            fato = fato[
                [
                    "pk_fato_acidente",
                    "num_acidente",
                    "pk_tempo",
                    "pk_localidade",
                    "pk_condicao_via",
                    "qtde_acidente",
                    "qtde_acid_com_obitos",
                    "qtde_envolvidos",
                    "qtde_feridosilesos",
                    "qtde_obitos",
                ]
            ]

            self.load_dataframe("fato_acidente", fato)

    def load_fato_vitima(
        self,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_vitima: pd.DataFrame,
    ) -> None:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["vitimas"])
        next_pk = 1

        columns = [
            "num_acidente",
            "data_acidente",
            "chv_localidade",
            "faixa_idade",
            "genero",
            "tp_envolvido",
            "gravidade_lesao",
            "equip_seguranca",
            "ind_motorista",
            "susp_alcool",
            "qtde_envolvidos",
            "qtde_feridosilesos",
            "qtde_obitos",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            df["data_acidente"] = pd.to_datetime(df["data_acidente"], errors="coerce")
            df["qtde_envolvidos"] = pd.to_numeric(df["qtde_envolvidos"], errors="coerce")
            df["qtde_feridosilesos"] = pd.to_numeric(df["qtde_feridosilesos"], errors="coerce")
            df["qtde_obitos"] = pd.to_numeric(df["qtde_obitos"], errors="coerce")

            df["flag_obito"] = df["qtde_obitos"] > 0
            df["flag_ferido"] = (
                (df["qtde_envolvidos"] > 1)
                & (df["qtde_feridosilesos"] < df["qtde_envolvidos"])
            )

            fato = df.merge(
                dim_tempo[["pk_tempo", "data"]],
                left_on="data_acidente",
                right_on="data",
                how="left",
            )

            fato = fato.merge(
                dim_localidade[["pk_localidade", "localidade"]],
                left_on="chv_localidade",
                right_on="localidade",
                how="left",
            )

            fato = fato.merge(
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

            fato = fato[
                [
                    "num_acidente",
                    "pk_tempo",
                    "pk_localidade",
                    "pk_vitima",
                    "flag_obito",
                    "flag_ferido",
                ]
            ].copy()

            for col in [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_vitima",
            ]:
                fato[col] = pd.to_numeric(fato[col], errors="coerce")

            self._validate_null_keys(
                fato,
                ["pk_tempo", "pk_localidade", "pk_vitima"],
                "fato_vitima",
            )

            fato = fato.reset_index(drop=True)
            fato["pk_fato_vitima"] = range(next_pk, next_pk + len(fato))
            next_pk += len(fato)

            fato = fato[
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

            self.load_dataframe("fato_vitima", fato)

    def load_fato_veiculo_acidente(
        self,
        acidente_lookup: pd.DataFrame,
        dim_tempo: pd.DataFrame,
        dim_localidade: pd.DataFrame,
        dim_tipo_veiculo: pd.DataFrame,
    ) -> None:
        parquet_parts = self._list_domain_parts(self.domain_folder_map["tipo_veiculo"])
        next_pk = 1

        columns = [
            "num_acidente",
            "tipo_veiculo",
            "ind_veic_estrangeiro",
            "qtde_veiculos",
        ]

        for parquet_file in parquet_parts:
            df = self._read_parquet_part(parquet_file, columns=columns)
            df["qtde_veiculos"] = pd.to_numeric(df["qtde_veiculos"], errors="coerce")

            fato = df.merge(
                acidente_lookup,
                on="num_acidente",
                how="left",
            )

            fato = fato.merge(
                dim_tempo[["pk_tempo", "data"]],
                left_on="data_acidente",
                right_on="data",
                how="left",
            )

            fato = fato.merge(
                dim_localidade[["pk_localidade", "localidade"]],
                left_on="chv_localidade",
                right_on="localidade",
                how="left",
            )

            fato = fato.merge(
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

            fato = fato[
                [
                    "num_acidente",
                    "pk_tempo",
                    "pk_localidade",
                    "pk_tipo_veiculo",
                    "qtde_veiculos",
                ]
            ].copy()

            for col in [
                "num_acidente",
                "pk_tempo",
                "pk_localidade",
                "pk_tipo_veiculo",
                "qtde_veiculos",
            ]:
                fato[col] = pd.to_numeric(fato[col], errors="coerce")

            self._validate_null_keys(
                fato,
                ["pk_tempo", "pk_localidade", "pk_tipo_veiculo"],
                "fato_veiculo_acidente",
            )

            fato = fato.reset_index(drop=True)
            fato["pk_fato_veiculo"] = range(next_pk, next_pk + len(fato))
            next_pk += len(fato)

            fato = fato[
                [
                    "pk_fato_veiculo",
                    "num_acidente",
                    "pk_tempo",
                    "pk_localidade",
                    "pk_tipo_veiculo",
                    "qtde_veiculos",
                ]
            ]

            self.load_dataframe("fato_veiculo_acidente", fato)

    def run(self, truncate_before_load: bool = True) -> None:
        self.client.test_connection()

        print("Running in FACTS-ONLY mode")

        dim_tempo = pd.read_sql(
            f"SELECT pk_tempo, data FROM {self.client.schema}.dim_tempo",
            self.client.engine,
        )
        dim_tempo["data"] = pd.to_datetime(dim_tempo["data"], errors="coerce")

        dim_localidade = pd.read_sql(
            f"SELECT pk_localidade, localidade FROM {self.client.schema}.dim_localidade",
            self.client.engine,
        )
        dim_localidade["localidade"] = dim_localidade["localidade"].astype(str)

        dim_condicao_via = pd.read_sql(
            f"""
            SELECT pk_condicao_via, fase_dia, lim_velocidade, tp_pista,
                ind_guardrail, ind_cantcentral, ind_acostamento
            FROM {self.client.schema}.dim_condicao_via
            """,
            self.client.engine,
        )

        dim_vitima = pd.read_sql(
            f"""
            SELECT pk_vitima, faixa_idade, genero, tp_envolvido,
                gravidade_lesao, equip_seguranca, ind_motorista, susp_alcool
            FROM {self.client.schema}.dim_vitima
            """,
            self.client.engine,
        )

        dim_tipo_veiculo = pd.read_sql(
            f"""
            SELECT pk_tipo_veiculo, tipo_veiculo, ind_veic_estrangeiro
            FROM {self.client.schema}.dim_tipo_veiculo
            """,
            self.client.engine,
        )

        acidente_lookup = self.build_acidente_lookup()
        acidente_lookup["data_acidente"] = pd.to_datetime(
            acidente_lookup["data_acidente"],
            errors="coerce",
        )

        if truncate_before_load:
            print("Truncating FACT tables only...")

            for table_name in [
                "fato_veiculo_acidente",
                "fato_vitima",
                "fato_acidente",
            ]:
                sql = (
                    f"TRUNCATE TABLE {self.client.schema}.{table_name} "
                    f"RESTART IDENTITY CASCADE;"
                )
                self.client.execute_sql(sql)

        print("Loading FACT tables...")

        self.load_fato_acidente(dim_tempo, dim_localidade, dim_condicao_via)
        self.load_fato_vitima(dim_tempo, dim_localidade, dim_vitima)
        self.load_fato_veiculo_acidente(
            acidente_lookup,
            dim_tempo,
            dim_localidade,
            dim_tipo_veiculo,
        )

        print("FACT tables loaded successfully.")
        
    # def run(self, truncate_before_load: bool = True) -> None:
    #     self.client.test_connection()

    #     dim_tempo = self.build_dim_tempo()
    #     dim_localidade = self.build_dim_localidade()
    #     dim_condicao_via = self.build_dim_condicao_via()
    #     dim_vitima = self.build_dim_vitima()
    #     dim_tipo_veiculo = self.build_dim_tipo_veiculo()
    #     acidente_lookup = self.build_acidente_lookup()

    #     if truncate_before_load:
    #         self.truncate_gold_tables()

    #     self.load_dataframe("dim_tempo", dim_tempo)
    #     self.load_dataframe("dim_localidade", dim_localidade)
    #     self.load_dataframe("dim_condicao_via", dim_condicao_via)
    #     self.load_dataframe("dim_vitima", dim_vitima)
    #     self.load_dataframe("dim_tipo_veiculo", dim_tipo_veiculo)

    #     self.load_fato_acidente(dim_tempo, dim_localidade, dim_condicao_via)
    #     self.load_fato_vitima(dim_tempo, dim_localidade, dim_vitima)
    #     self.load_fato_veiculo_acidente(
    #         acidente_lookup,
    #         dim_tempo,
    #         dim_localidade,
    #         dim_tipo_veiculo,
    #     )

    #     print("All gold tables were loaded successfully into PostgreSQL.")

if __name__ == "__main__":
    loader = SilverToGoldIngestion(silver_dir="data/silver")
    loader.run(truncate_before_load=True)