from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import os, re, logging, json, yaml, requests
from delta import *
from pyspark.sql import DataFrame, functions as F

from datetime import datetime
from logging import Logger
import builtins

from pacifico_utils import query_transformacion, execute_sp, init_logging, get_secret, get_current_date, ctrl_pipeline_detail_delta, register_end_execution_azql,load_general_parameters,load_file,get_project_root

# Get dbutils from databricks
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def get_view_pipeline_configuration_out(
    table_id: int,
    module_name: str,
) -> str:
    """
    load table save path according to the control table active pipelines
    Args:
        table_id: table pipeline id
        module_name: table module ex. RAW, UNIVERSAL
    Returns:
        Save path
    """
    # column_name=column_name.lower()

    df_vista= query_transformacion(
        "SELECT * FROM [sch_pacifico_da_ctrl_az].[vw_pipeline_configuration_out]"
    ).filter((F.col("pipeline_id") == table_id) & (F.col("module_name") == module_name))

    dict_valor = json.loads(df_vista.toJSON().first())
    return dict_valor

#Metodo usado para recuperar el dataframe de la tabla DDV usando el schema declarado en la tabla tbl_pipeline_parameter
def load_path_pipeline_dependency(src_name, dict_dependencys):
    path_raiz = load_general_parameters(key="prm_path_raiz")
    src_path = dict_dependencys[src_name]["src_path"]
    src_type_format = dict_dependencys[src_name]["src_type_format"]
    src_module = dict_dependencys[src_name]["src_module_name"]
    if "PARQUET" in src_type_format:
        src_type_format = "PARQUET_DEFAULT"
        return load_file(f"{path_raiz}/{src_path}", src_type_format)
    if "DELTA" in src_type_format:
        return load_file(f"{path_raiz}/{src_path}", src_type_format)

def load_path_uc_dependency(src_name, dict_dependencys,source_catalog):
    src_schema_delta = dict_dependencys[src_name]["src_schema_delta"]
    return spark.sql(f"select * from {source_catalog}.{src_schema_delta}.{src_name}")

def configurar_credenciales(target_storage_account_name:str, env:str)->str:
    workspace=get_project_root()
    # Cargar el archivo YAML con las configuraciones
    directory = f"{workspace}/pacifico_params/storage_account_params.yml"
    with open(directory, "r") as f:
        config = yaml.full_load(f.read())
    
    # Obtener las credenciales para el storage account y ambiente especificados
    credenciales = config[env][target_storage_account_name]
    client_id = get_secret(scope=credenciales['scope_secret'], key=credenciales['client_id_key_secret'])
    client_secret = get_secret(scope=credenciales['scope_secret'], key=credenciales['client_secret_key_secret'])
    tenant_id=credenciales['tenant_id']
    storage_account_name=credenciales["storage_account_name"]
    
    # Configurar las credenciales en Spark
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
    return storage_account_name+".dfs.core.windows.net"

def apply_transformations_df(df:DataFrame, config:dict):
    """
    Permite enriquecer/transformar el dataset origen (df) dependiendo la salida deseada.

    Parámetros:
    - df: DataFrame de PySpark a transformar.
    - config: Configuración de columnas a reordenar, renombrar o filtros a usarse.
    """
    # Seleccionar y reordenar columnas
    # Preparar la selección de columnas y renombramiento
    if 'columns' in config:
        if config['columns'] == ['*']:  # Si se deben seleccionar todas las columnas
            selected_columns = [F.col(c).alias(config.get('rename_columns', {}).get(c, c)) for c in df.columns]
        else:
            selected_columns = [F.col(c).alias(config.get('rename_columns', {}).get(c, c)) for c in config['columns']]
        df = df.select(*selected_columns)
    
    # Aplicar filtros
    if 'filters' in config:
        for filter in config['filters']:
            for column, conditions in filter.items():
                for condition in conditions:
                    # Se configura el type: in cuando se tiene una lista de valores a filtrar
                    if condition['type'] == 'in':
                        df = df.filter(F.col(column).isin(condition['values']))
                    # Se configura el type: not in para excluir una lista de valores
                    elif condition['type'] == 'not_in':
                        df = df.filter(~F.col(column).isin(condition['values']))
                    elif condition['type'] == 'greater_or_equal':
                        df = df.filter(F.col(column) >= condition['value'])
                    elif condition['type'] == 'equal':
                        df = df.filter(F.col(column) == condition['value'])
                    elif condition['type'] == 'not_equal':
                        df = df.filter(F.col(column) != condition['value'])
    
    return df

def get_environment(adb_cluster_name:str):
    if "desa" in adb_cluster_name.lower():
        env="dev"
    else:
        env="prd"
    --return env
