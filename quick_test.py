#!/usr/bin/env python3
import cdm_parser_v2

parser = cdm_parser_v2.CDMParser("/root/pump_parser/uploads/CDM_CDMF_241125.pdf")
result = parser.parse()

print(f"\n=== РЕЗУЛЬТАТЫ ПАРСИНГА ===")
print(f"Всего записей: {len(result)}")

if result:
    print(f"\n=== ПЕРВЫЕ 5 ЗАПИСЕЙ ===")
    for i, pump in enumerate(result[:5]):
        print(f"{i+1}. {pump}")

    # Группируем по моделям
    models = set([p['id'] for p in result])
    print(f"\n=== УНИКАЛЬНЫХ МОДЕЛЕЙ: {len(models)} ===")
    print("Примеры моделей:", list(models)[:10])
