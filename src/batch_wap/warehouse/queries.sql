create database glue
engine = DataLakeCatalog
settings 
    catalog_type = 'glue',
    region = 'us-east-1',
    aws_access_key_id = '...',
    aws_secret_access_key = '...';

select * from glue.`batch_wap.raw_events`;
