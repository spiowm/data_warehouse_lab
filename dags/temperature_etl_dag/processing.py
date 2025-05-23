# dags/temperature_etl_dag/processing.py
import pandas as pd
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Tuple
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

# Імпорт допоміжних функцій та визначень датасетів
from temperature_etl_dag.helpers import get_season_en, get_hemisphere_en, get_continent_en, slugify_column_names
from temperature_etl_dag.datasets_definition import PROCESSED_DATASETS_FULL_PATH


def transform_and_save_data(**context):
    """
    Головна функція для ETL процесу: читає CSV, трансформує дані
    відповідно до моделі з Лаб1, та зберігає у JSON файли.
    """
    # Локальні словники для генерації Surrogate Keys - краще ніж глобальні для розподілених тасків Airflow
    date_to_sk_map: Dict[str, int] = {}
    city_to_sk_map: Dict[Tuple[str, str], int] = {}
    next_date_sk = 1
    next_city_sk = 1

    print(f"Починаємо трансформацію даних для запуску DAG на {context['dag_run'].logical_date}...")

    # 1. Отримання шляхів та імен файлів
    data_folder_conn = BaseHook.get_connection('temperature_data_folder_conn')
    base_data_path = str(json.loads(data_folder_conn.extra)['path'])

    # Явне перетворення у рядок для уникнення проблем з типами
    raw_filename = str(Variable.get('raw_temperature_input_filename'))
    
    # Використовуємо Path для правильної обробки шляхів
    full_raw_file_path = str(Path(base_data_path) / raw_filename)

    print(f"Читаємо вхідні дані з: {full_raw_file_path}")
    print(f"Зберігатимемо оброблені дані у: {PROCESSED_DATASETS_FULL_PATH}")

    # Переконуємося, що папка для вихідних файлів існує
    os.makedirs(PROCESSED_DATASETS_FULL_PATH, exist_ok=True)

    # 2. Читання CSV
    try:
        # Вказуємо типи даних для уникнення проблем з парсингом
        dtype_spec = {
            'AverageTemperature': 'float',
            'AverageTemperatureUncertainty': 'float',
            'City': 'str',
            'Country': 'str',
            'Latitude': 'str',
            'Longitude': 'str'
        }
        raw_df = pd.read_csv(full_raw_file_path, parse_dates=['dt'], dtype=dtype_spec)
        # Приводимо назви колонок до єдиного стилю (snake_case)
        raw_df.columns = slugify_column_names(raw_df.columns)
        print(f"Успішно прочитано {len(raw_df)} рядків з CSV.")
    except Exception as e:
        print(f"ПОМИЛКА: Не вдалося прочитати CSV файл. Деталі: {e}")
        raise

    # 3. Трансформація даних та підготовка датасетів
    dim_date_records: List[Dict[str, Any]] = []
    dim_city_records: List[Dict[str, Any]] = []
    fact_temperatures_records: List[Dict[str, Any]] = []

    for _, row in raw_df.iterrows():
        # --- DimDate ---
        date_val = row.get('dt')  # Використовуємо .get для безпечного доступу
        if pd.isna(date_val):
            continue

        # Форматуємо дату у рядок для використання як ключ
        full_date_str = date_val.strftime('%Y-%m-%d')
        
        # Додаємо запис у вимір дат, якщо такої дати ще немає
        if full_date_str not in date_to_sk_map:
            date_to_sk_map[full_date_str] = next_date_sk
            dim_date_records.append({
                'DateSK': next_date_sk,
                'FullDate': full_date_str,
                'Year': date_val.year,
                'Month': date_val.month,
                'MonthName': date_val.strftime('%B'),
                'DayOfMonth': date_val.day,
                'Quarter': date_val.quarter,
                'Season': get_season_en(date_val.month),  # Визначаємо пору року за місяцем
                'Decade': f"{date_val.year // 10 * 10}s"  # Десятиліття, наприклад, "1980s"
            })
            next_date_sk += 1
        current_date_sk = date_to_sk_map[full_date_str]

        # --- DimCity ---
        city_name = str(row.get('city', "Unknown"))
        country_name = str(row.get('country', "Unknown"))
        city_key = (city_name, country_name)  # Унікальний ключ для міста
        
        lat_str = str(row.get('latitude', ""))
        lon_str = str(row.get('longitude', ""))

        # Парсинг значення широти з текстового формату (наприклад, "57.05N" -> 57.05)
        lat_val_num = None
        if lat_str:
            try:
                val = float(lat_str[:-1])
                if lat_str[-1].upper() == 'S': val *= -1  # Південна півкуля має від'ємні значення
                lat_val_num = round(val, 6)  # Округляємо для уніфікації
            except ValueError:
                pass  # Якщо не вдається розпарсити, залишаємо None

        # Парсинг значення довготи
        lon_val_num = None
        if lon_str:
            try:
                val = float(lon_str[:-1])
                if lon_str[-1].upper() == 'W': val *= -1  # Західна півкуля має від'ємні значення
                lon_val_num = round(val, 6)
            except ValueError:
                pass

        # Додаємо запис у вимір міст, якщо такого міста ще немає
        if city_key not in city_to_sk_map:
            city_to_sk_map[city_key] = next_city_sk
            dim_city_records.append({
                'CitySK': next_city_sk,
                'CityName': city_name,
                'CountryName': country_name,
                'Latitude_str': lat_str,  # Оригінальний текстовий формат
                'Longitude_str': lon_str,
                'Latitude_val': lat_val_num,  # Числове значення для зручності аналізу
                'Longitude_val': lon_val_num,
                'Continent': get_continent_en(country_name),  # Визначаємо континент за країною
                'Hemisphere': get_hemisphere_en(lat_val_num)  # Визначаємо півкулю за широтою
            })
            next_city_sk += 1
        current_city_sk = city_to_sk_map[city_key]

        # --- FactMonthlyTemperatures ---
        avg_temp = row.get('averagetemperature')
        avg_temp_unc = row.get('averagetemperatureuncertainty')

        # Додаємо факт тільки якщо є значення температури
        if pd.notna(avg_temp):
            fact_temperatures_records.append({
                'DateSK_FK': current_date_sk,  # Зовнішній ключ до DimDate
                'CitySK_FK': current_city_sk,  # Зовнішній ключ до DimCity
                'AverageTemperature': round(avg_temp, 3) if pd.notna(avg_temp) else None,
                'AverageTemperatureUncertainty': round(avg_temp_unc, 3) if pd.notna(avg_temp_unc) else None
            })

    # 4. Збереження датасетів у JSON файли
    datasets = [
        {'name': 'dim_date.json', 'data': dim_date_records},
        {'name': 'dim_city.json', 'data': dim_city_records},
        {'name': 'fact_monthly_temperatures.json', 'data': fact_temperatures_records}
    ]
    
    # Зберігаємо кожен датасет
    for dataset in datasets:
        # Використовуємо Path для правильної роботи з шляхами
        output_path = str(Path(PROCESSED_DATASETS_FULL_PATH) / dataset['name'])
        
        # спочатку створюємо JSON-рядок, потім записуємо
        json_str = json.dumps(dataset['data'], indent=4, ensure_ascii=False)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
            
        print(f"Збережено {len(dataset['data'])} записів у {output_path}")

    print("Трансформацію та збереження даних завершено.")
