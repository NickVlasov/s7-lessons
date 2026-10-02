# Data Lake — Геоаналитика соцсети

## Структура хранилища

### Исходные данные (Raw)
```
/user/master/data/geo/events/          # Таблица событий (Parquet, партиция date=)
/user/s30080726/data/geo.csv            # Справочник городов Австралии (CSV)
```

### Рабочая директория
```
/user/s30080726/data/
├── test/                               # Отладка и семплы
├── analytics/                          # Данные для аналитиков
│   ├── geo_events/                      # События с привязкой к городу (ODS)
├── marts/                              # Витрины
│   ├── user_mart/                       # Витрина пользователей
│   ├── zone_mart/                       # Витрина зон
│   └── friend_recommendation_mart/      # Рекомендации друзей
```

### Форматы
- Исходные события: **Parquet**, партиционирование по `date`
- geo.csv: **CSV** (разделитель `;`, десятичный разделитель — запятая)
- ODS и витрины: **Parquet**

### Частота обновления
| Слой      | Витрина                       | Частота       |
| ODS       | geo_events                    | Ежедневно     |
| Datamarts | user_mart                     | Ежедневно     |
| Datamarts | zone_mart                     | Еженедельно   |
| Datamarts | friend_recommendation_mart    | Ежедневно     |

### Автоматизация
```
dags/
├── geo_analytics_dag.py                # DAG в Airflow
scripts/
├── user_mart.py                         # Скрипт витрины пользователей
├── zone_mart.py                         # Скрипт витрины зон
└── friend_recommendation_mart.py         # Скрипт рекомендаций друзей
```
