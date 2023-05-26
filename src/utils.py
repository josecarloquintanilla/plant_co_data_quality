import pickle
import pandas as pd
import awswrangler as wr
import os
import uuid
import boto3

def dump_pickle(data, name):
    with open(name, 'wb') as file_features:
        pickle.dump(data, file_features, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(name):
    with open(name, 'rb') as data:
        return pickle.load(data)

def get_data_from_datalake(request, database="DEFAULT"):
    """Executing the given query to fetch data from the datalake

        Parameters
        ----------
        request : str
            AWS Athena request that need to be executed
        database : str
            Database name where the data need to be fetch by default set to "DEFAULT"
        Returns
        -------
        data : Pandas DataFrame
            Data fetch from the datalake in dataframe format
    """
    data = wr.athena.read_sql_query(
        sql=request,
        database=database,
        ctas_approach=True,
        ctas_temp_table_name=f'dmdg_temp_table_{uuid.uuid4()}',
    )
    return data

def write_into_table(df, table_name, workspace_prefix, workspace_bucket=f'sanofi-datalake-dmdg-workspaces-prod', mode='overwrite', preserve_index=False):
    """Write a dataframe into datalake tables (can handle both prod-dmdg-business and dedicated workspace)

        Parameters
        ----------
        df : Pandas DataFrame
            Dataframe that needs to be turn into table
        table_name : str
            Table name to be created
        database_name : str
            Database name to be use for creating table by default set to 'prod-dmdg-business'
        mode : str
            Mode of creation can be overwrite, append, overwrite_partitions by default set to 'overwrite'
        workspace_bucket : str
            Name of workspace bucket of the dmdg workspace
        workspace_prefix : str
            Name of workspace prefix of the dmdg workspace
        preserve_index : boolean
            Boolean that specify if you preserve ornot the index by default set to False
    """
    path = f's3://{workspace_bucket}/{workspace_prefix}/__TABLES__/{table_name}'
    database_name = 'prod-dmdg-workspace-%s' % workspace_prefix
    wr.s3.to_parquet(
                        df=df,
                        path=path,
                        database=database_name,
                        dataset=True,
                        table=table_name,
                        mode=mode,
                        index=preserve_index,
                        compression=None
                    )
