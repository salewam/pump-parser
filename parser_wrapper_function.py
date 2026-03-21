
# Функция-обёртка для совместимости с app.py
def extract_cdm_from_pdf(pdf_path):
    """
    Извлекает данные о насосах CDM из PDF файла.

    Args:
        pdf_path: путь к PDF файлу

    Returns:
        Список словарей с данными насосов в формате:
        {"id": "CDM1-2", "kw": 0.37, "q": 0.5, "head_m": 11.5}
    """
    parser = CDMParser(pdf_path)
    return parser.parse()
