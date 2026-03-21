#!/usr/bin/env python3
import cdm_parser_v2
import json

# Парсим PDF
print("\n=== ЗАПУСК ПАРСЕРА ===")
parser = cdm_parser_v2.CDMParser("/root/pump_parser/uploads/CDM_CDMF_241125.pdf")
result = parser.parse()

# Показываем первые 10 записей
print("\n=== ПЕРВЫЕ 10 ЗАПИСЕЙ ===")
for i, pump in enumerate(result[:10]):
    print(f"{i+1}. {json.dumps(pump, ensure_ascii=False)}")

print(f"\n=== ВСЕГО: {len(result)} записей ===")

# Группируем по моделям
models = {}
for pump in result:
    model_id = pump["id"]
    if model_id not in models:
        models[model_id] = 0
    models[model_id] += 1

print(f"\n=== УНИКАЛЬНЫХ МОДЕЛЕЙ: {len(models)} ===")
print("\nПримеры моделей:")
for model_id in list(models.keys())[:10]:
    print(f"  {model_id}: {models[model_id]} точек")
