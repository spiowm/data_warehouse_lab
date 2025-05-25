# dags/temperature_etl_dag/processing.py
import pandas as pd
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Tuple
from airflow.sdk import Variable
from airflow.hooks.base import BaseHook

# Імпорт допоміжних функцій та визначень датасетів
from temperature_etl_dag.helpers import (
    get_season_en, get_hemisphere_en, slugify_column_names,
    get_continent_name_basic
)
from temperature_etl_dag.datasets_definition import PROCESSED_DATASETS_FULL_PATH


def transform_and_save_data(**context):
    """
    Головна функція для ETL процесу: читає CSV, трансформує дані
    відповідно до моделі з Лаб1, та зберігає у JSON файли.
    """
    date_to_sk_map: Dict[str, int] = {}
    city_to_sk_map: Dict[Tuple[str, str], int] = {}
    next_date_sk = 1
    next_city_sk = 1

    print(f"Починаємо трансформацію даних для запуску DAG на {context['dag_run'].logical_date}...")

    data_folder_conn = BaseHook.get_connection('temperature_data_folder_conn')
    base_data_path = str(json.loads(data_folder_conn.extra)['path'])
    raw_filename = str(Variable.get('raw_temperature_input_filename'))
    full_raw_file_path = str(Path(base_data_path) / raw_filename)

    print(f"Читаємо вхідні дані з: {full_raw_file_path}")
    print(f"Зберігатимемо оброблені дані у: {PROCESSED_DATASETS_FULL_PATH}")

    os.makedirs(PROCESSED_DATASETS_FULL_PATH, exist_ok=True)

    try:
        dtype_spec = {
            'AverageTemperature': 'float',
            'AverageTemperatureUncertainty': 'float',
            'City': 'str',
            'Country': 'str',
            'Latitude': 'str',
            'Longitude': 'str'
        }
        # keep_default_na=False та na_values=[''] щоб пусті рядки читалися як пусті рядки, а не NaN, якщо це важливо
        # для City/Country перед перетворенням на str().
        raw_df = pd.read_csv(full_raw_file_path, parse_dates=['dt'], dtype=dtype_spec, keep_default_na=False, na_values=[''])
        raw_df.columns = slugify_column_names(raw_df.columns)
        print(f"Успішно прочитано {len(raw_df)} рядків з CSV.")
    except Exception as e:
        print(f"ПОМИЛКА: Не вдалося прочитати CSV файл. Деталі: {e}")
        raise

    dim_date_records: List[Dict[str, Any]] = []
    dim_city_records: List[Dict[str, Any]] = []
    fact_temperatures_records: List[Dict[str, Any]] = []

    for _, row in raw_df.iterrows():
        date_val = row.get('dt')
        if pd.isna(date_val):
            # print(f"Пропущено рядок через відсутню дату: {row.to_dict()}") # Можна розкоментувати для дебагу
            continue

        full_date_str = date_val.strftime('%Y-%m-%d')
        if full_date_str not in date_to_sk_map:
            date_to_sk_map[full_date_str] = next_date_sk
            year_val = date_val.year
            month_val = date_val.month
            dim_date_records.append({
                'date_sk': next_date_sk,
                'full_date': full_date_str,
                'year': year_val,
                'month': month_val,
                'month_name': date_val.strftime('%B'),
                'year_month': int(f"{year_val}{month_val:02d}"),
                'quarter': date_val.quarter,
                'season': get_season_en(month_val),
                'decade': (year_val // 10) * 10
            })
            next_date_sk += 1
        current_date_sk = date_to_sk_map[full_date_str]

        city_name_raw = row.get('city')
        country_name_raw = row.get('country')

        # Перевіряємо, чи значення не є NaN перед перетворенням на str
        # Пусті рядки теж можуть бути проблемою, якщо вони мають бути NULL або оброблені інакше
        if pd.isna(city_name_raw) or str(city_name_raw).strip() == "" or \
                pd.isna(country_name_raw) or str(country_name_raw).strip() == "":
            # print(f"Пропущено рядок через відсутнє місто/країну: {row.to_dict()}") # Можна розкоментувати для дебагу
            continue

        city_name = str(city_name_raw).strip()
        country_name = str(country_name_raw).strip()
        city_key = (city_name, country_name)

        lat_str = str(row.get('latitude', ""))  # Забезпечуємо, що це рядок
        lon_str = str(row.get('longitude', ""))  # Забезпечуємо, що це рядок

        lat_val_num = None
        if lat_str.strip() and lat_str.strip()[-1].upper() in ('N', 'S'):
            try:
                val_numeric = float(lat_str.strip()[:-1])
                lat_val_num = round(val_numeric if lat_str.strip()[-1].upper() == 'N' else -val_numeric, 6)
            except (ValueError, TypeError, IndexError):
                pass

        lon_val_num = None
        if lon_str.strip() and lon_str.strip()[-1].upper() in ('E', 'W'):
            try:
                val_numeric = float(lon_str.strip()[:-1])
                lon_val_num = round(val_numeric if lon_str.strip()[-1].upper() == 'E' else -val_numeric, 6)
            except (ValueError, TypeError, IndexError):
                pass

        if city_key not in city_to_sk_map:
            city_to_sk_map[city_key] = next_city_sk
            dim_city_records.append({
                'city_sk': next_city_sk,
                'city_name': city_name,
                'country_name': country_name,
                'latitude_val': lat_val_num,
                'longitude_val': lon_val_num,
                'continent_name': get_continent_name_basic(country_name),
                'hemisphere': get_hemisphere_en(lat_val_num) if lat_val_num is not None else "Unknown"
            })
            next_city_sk += 1
        current_city_sk = city_to_sk_map[city_key]

        avg_temp_raw = row.get('averagetemperature')
        avg_temp_unc_raw = row.get('averagetemperatureuncertainty')

        avg_temp = None
        try:  # Спроба конвертувати в float, якщо це можливо
            if pd.notna(avg_temp_raw):
                avg_temp = round(float(avg_temp_raw), 3)
        except (ValueError, TypeError):
            pass  # Залишаємо None, якщо конвертація не вдалася

        avg_temp_unc = None
        try:  # Спроба конвертувати в float, якщо це можливо
            if pd.notna(avg_temp_unc_raw):
                avg_temp_unc = round(float(avg_temp_unc_raw), 3)
        except (ValueError, TypeError):
            pass  # Залишаємо None

        if avg_temp is not None:
            fact_temperatures_records.append({
                'date_sk': current_date_sk,
                'city_sk': current_city_sk,
                'average_temperature_celsius': avg_temp,
                'average_temperature_uncertainty': avg_temp_unc
            })

    # 4. Збереження датасетів у JSON файли
    # Логіка збереження залишається такою ж, але тепер вона буде зберігати файли згідно з НОВИМИ datasets_definition
    datasets_to_save_info = [
        {'name': 'dim_date.json', 'data': dim_date_records},
        {'name': 'dim_city.json', 'data': dim_city_records},
        {'name': 'fact_monthly_temperatures.json', 'data': fact_temperatures_records}
    ]

    # Використовуємо outlets для визначення шляхів збереження, як це було визначено в DAG
    # Порядок outlets у DAG має співпадати з порядком тут
    current_task_object = context['dag'].get_task(context['task_instance'].task_id)
    defined_outlets = current_task_object.outlets

    for i, item_info in enumerate(datasets_to_save_info):
        dataset_obj = defined_outlets[i]
        # Переконуємось, що ім'я файлу з Dataset об'єкту співпадає з очікуваним
        expected_filename = item_info['name']
        actual_filename_from_uri = Path(dataset_obj.uri).name

        if actual_filename_from_uri != expected_filename:
            print(f"ПОПЕРЕДЖЕННЯ: Ім'я файлу з URI датасету ({actual_filename_from_uri}) не співпадає з очікуваним ({expected_filename}). Використовується URI: {dataset_obj.uri}")

        output_path = dataset_obj.uri.replace("file://", "")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        json_str = json.dumps(item_info['data'], indent=4, ensure_ascii=False)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
        print(f"Збережено {len(item_info['data'])} записів у {output_path} (Dataset: {dataset_obj.uri})")

    print("Трансформацію та збереження даних завершено.")
