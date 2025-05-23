# dags/temperature_etl_dag/helpers.py
import re

def get_season_en(month: int) -> str:
    """
    Визначає пору року англійською мовою за номером місяця.
    Припускаємо північну півкулю для простоти.
    """
    if month in [12, 1, 2]:
        return "Winter"
    if month in [3, 4, 5]:
        return "Spring"
    if month in [6, 7, 8]:
        return "Summer"
    if month in [9, 10, 11]:
        return "Autumn"
    return "Unknown"

def get_hemisphere_en(lat_val: float) -> str:
    """
    Визначає півкулю англійською мовою за значенням широти.
    """
    if lat_val is None:
        return "Unknown"
    return "Northern" if lat_val >= 0 else "Southern"

def slugify_column_names(columns_list: list) -> list:
    """
    Приводить назви колонок до snake_case (малі літери, пробіли та тире замінені на підкреслення).
    """
    return [re.sub(r'[\s-]+', '_', col.lower().strip()) for col in columns_list]

def get_continent_name_basic(country_name: str) -> str:
    """
    Дуже базова логіка для визначення континенту за назвою країни. Заглушка.
    У реальному проєкті тут був би lookup до довідника.
    """
    # Приклад дуже спрощеної логіки (потребує значного розширення)
    european_countries = ["Denmark", "Germany", "France", "United Kingdom", "Ukraine", "Turkey"]
    asian_countries = ["China", "India", "Japan", "Turkey"]
    north_american_countries = ["United States", "Canada", "Mexico"]

    if country_name in european_countries:
        return "Europe"
    elif country_name in asian_countries:
        return "Asia"
    elif country_name in north_american_countries:
        return "North America"
    # ... додати більше
    return "Unknown" # Або "N/A" якщо дані відсутні