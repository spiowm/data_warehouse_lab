# dags/temperature_etl_dag/helpers.py
def get_season_en(month: int) -> str:
    """
    Визначає пору року англійською мовою за номером місяця.
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

def get_continent_en(country_name: str) -> str:
    """
    У реальному проекті це був би довідник або API.
    Для лабораторної базової логіки або "N/A".
    """
    # Це дуже спрощено. Можна додати кілька країн для прикладу.
    # european_countries = ["Denmark", "Germany", "France", "United Kingdom", "Ukraine"]
    # if country_name in european_countries:
    #     return "Europe"
    # Додайте інші за потреби
    return "N/A" # Not Applicable / Unknown

def slugify_column_names(columns_list: list) -> list:
    """
    Приводить назви колонок до snake_case (малі літери, пробіли та тире замінені на підкреслення).
    """
    import re
    return [re.sub(r'[\s-]+', '_', col.lower().strip()) for col in columns_list]