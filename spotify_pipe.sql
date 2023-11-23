create database spotify_db



create or replace table tblalbum(
    album_id varchar(50),
    album_name varchar(100),
    album_release_date date,
    album_total_tracks int,
    album_url varchar(120)
);

create or replace table tblsongs(
    song_id varchar(50),
    song_name varchar(60),
    song_duration varchar(10),
    song_url varchar(120),
    song_popularity int,
    song_added date,
    album_id varchar(50),
    artist_id varchar(50)
    
);

create or replace table tblartist(
    artist_id varchar(50),
    artist_name varchar (50),
    external_url varchar(120)
)

create storage integration s3_spotify
    type= external_stage
    storage_provider = 's3'
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::835395167012:role/snowflake-s3'
    storage_allowed_locations = ('s3://spotify-etl-pipeline-de/transformed_data/')

desc integration s3_spotify

create or replace schema spotify_db.file_formats

create or replace file format spotify_db.file_formats.csv
    type=csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

create or replace schema spotify_db.external_stages

create or replace stage spotify_db.external_stages.aws_stage
    url='s3://spotify-etl-pipeline-de/transformed_data/'
    storage_integration = s3_spotify
    file_format = spotify_db.file_formats.csv    

list @spotify_db.external_stages.aws_stage

copy into spotify_db.public.tblsongs
    from @spotify_db.external_stages.aws_stage/songs_data

select * from spotify_db.public.tblartist

create or replace schema spotify_db.pipes



create or replace pipe spotify_db.pipes.artist_pipe
auto_ingest = TRUE
as
copy into spotify_db.public.tblartist
from @spotify_db.external_stages.aws_stage/artist_data

desc pipe spotify_db.pipes.artist_pipe
select * from spotify_db.public.tblsongs
truncate table spotify_db.public.tblalbum

create or replace pipe spotify_db.pipes.album_pipe
auto_ingest = TRUE
as
copy into spotify_db.public.tblalbum
from @spotify_db.external_stages.aws_stage/album_data

desc pipe spotify_db.pipes.album_pipe

create or replace pipe spotify_db.pipes.songs_pipe
auto_ingest = TRUE
as
copy into spotify_db.public.tblsongs
from @spotify_db.external_stages.aws_stage/songs_data

desc pipe spotify_db.pipes.songs_pipe

show pipes
