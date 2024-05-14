import argparse
import boto3
import pandas as pd
from dotenv import dotenv_values
from sklearn.model_selection import train_test_split

BUCKET_NAME = 'pabd24'
YOUR_ID = '27'
CSV_PATHS = ['data/raw/1_2024-05-14_19-09.csv',
             'data/raw/2_2024-05-14_19-10.csv',
             'data/raw/3_2024-05-14_19-10.csv']

config = dotenv_values(".env")

def main(args):
    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=config['KEY'],
        aws_secret_access_key=config['SECRET']
    )

    # Инициализация пустого DataFrame
    df = pd.DataFrame()

    # Загрузка и чтение каждого файла в DataFrame
    for csv_path in args.input:
        # Замена обратных слешей на прямые
        csv_path = csv_path.replace("\\", "/")
        object_name = f'{YOUR_ID}/{csv_path}'
        s3.download_file(BUCKET_NAME, object_name, csv_path)
        temp_df = pd.read_csv(csv_path)
        # Добавление временного DataFrame к основному DataFrame
        df = pd.concat([df, temp_df], ignore_index=True)

    # Выполнение разделения на обучающую и тестовую выборки
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

    # Сохранение обучающего и тестового DataFrame в CSV файлы
    train_df.to_csv('train.csv', index=False)
    test_df.to_csv('test.csv', index=False)

    return train_df, test_df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', nargs='+',
                        help='Входные серверные файлы для загрузки из S3',
                        default=CSV_PATHS)
    args = parser.parse_args()
    train_df, test_df = main(args)
    print("Обучающий DataFrame сохранен в train.csv")
    print("Тестовый DataFrame сохранен в test.csv")
