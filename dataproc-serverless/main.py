from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import argparse


def extract(spark, conf):
    """
    Loads the data from multiple sources like gcs, bigquery, etc. into a DataFrame
    :param spark: SparkSession
    :param conf: Input config
    :return: DataFrame
    """
    if conf['type'] == 'gcs':
        if conf['format'] == 'csv':
            return spark.read.option('header', 'true').option('inferSchema', 'true').csv(conf['name'])
        elif conf['format'] == 'parquet':
            return spark.read.parquet(conf['name'])
        else:
            raise Exception(f"{conf['format']} file format not implemented")
    elif conf['type'] == 'bigquery':
        return spark.read.format('bigquery').option('table', conf['name']).load()
    else:
        raise Exception(f"{conf['type']} input type not implemented")


def raw2refined(spark, conf):
    """
    Cleans/enriches the inputs to add business value
    :param spark: SparkSession
    :param conf: Transformation config
    :return: Dictionary of DataFrames
    """
    df_movies = extract(spark, conf['movies']['input'])
    return {'movies': df_movies.filter(f.col('genres').contains('Adventure')).drop('genres')}


def refined2integrated(spark, conf):
    """
    Integrates two dataframes
    :param spark: SparkSession
    :param conf: Transformation config
    :return: Dictionary of DataFrames
    """
    df_movies = extract(spark, conf['inputs']['movies'])
    df_ratings = extract(spark, conf['inputs']['ratings'])
    return {'movie_ratings': df_movies.join(df_ratings, ['movieId'], 'inner').select('userId', 'movieId', 'title',
                                                                                     'rating')}


def transform(spark, layer, r2r_conf, r2i_conf):
    """
    Invokes the transformation methods based on the layer
    :param spark: SparkSession
    :param layer: Different layers, e.g. raw2refined
    :param r2r_conf: raw to refined config
    :param r2i_conf: refined to integrated config
    :return: Transformed DataFrame
    """
    if layer == 'raw2refined':
        return raw2refined(spark, r2r_conf)
    elif layer == 'refined2integrated':
        return refined2integrated(spark, r2i_conf)
    else:
        raise Exception(f'{layer} layer not implemented')


def load(df, conf):
    """
    Writes the DataFrame to the defined target
    :param df: DataFrame to write
    :param conf: output config
    :return: None
    """
    if conf['type'] == 'gcs':
        if conf['format'] == 'csv':
            df.write.mode('overwrite') \
                .option('temporaryGcsBucket', 'gs-dataproc-staging') \
                .option('header', 'true').csv(conf['name'])
        elif conf['format'] == 'parquet':
            df.write.mode('overwrite') \
                .option('temporaryGcsBucket', 'gs-dataproc-staging') \
                .parquet(conf['name'])
        else:
            raise Exception(f" {conf['format']} file format not implemented")
    elif conf['type'] == 'bigquery':
        df.write.mode('overwrite').format('bigquery') \
            .option('temporaryGcsBucket', 'gs-dataproc-staging') \
            .option('table', conf['name']).save()
    else:
        raise Exception(f"{conf['type']} input type not implemented")


def main(conf):
    # create Spark Session
    spark = SparkSession.builder.appName(conf.app_name).getOrCreate()
    # transform data
    r2r_conf = {
        'movies': {
            'input': {
                'name': 'gs://gs-raw/movies/',
                'type': 'gcs',
                'format': 'csv'
            },
            'output': {
                'name': 'refined.movies',
                'type': 'bigquery'
            }
        }
    }
    r2i_conf = {
        'inputs': {
            'movies': {
                'name': 'refined.movies',
                'type': 'bigquery'
            },
            'ratings': {
                'name': 'refined.ratings',
                'type': 'bigquery'
            }
        },
        'outputs': {
            'movie_ratings': {
                'name': 'integrated.movie_ratings',
                'type': 'bigquery'
            }
        }
    }
    df_transformed = transform(spark, conf.layer, r2r_conf, r2i_conf)
    # save data
    for key in df_transformed.keys():
        if conf.layer == 'raw2refined':
            load(df_transformed[key], r2r_conf[key]['output'])
        elif conf.layer == 'refined2integrated':
            load(df_transformed[key], r2i_conf['outputs'][key])


if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '--app-name',
        dest='app_name',
        type=str,
        default='default',
        help='Application name'
    )
    parser.add_argument(
        '--layer',
        dest='layer',
        type=str,
        default='raw2refined',
        choices=['raw2refined', 'refined2integrated'],
        help='Job layer - raw2refined, refined2integrated'
    )
    args = parser.parse_args()
    main(args)
