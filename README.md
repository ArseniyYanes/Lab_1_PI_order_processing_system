# Лабораторная работа — Система обработки заказов

Командная работа (2 человека). Веб-страница с формой заказа → Python (Flask) → MySQL + Kafka + RabbitMQ. Всё поднимается через `docker-compose.yml`; наружу выводится `nginx`, который отдаёт HTML-форму и проксирует `/api/*` на Flask-обработчик.

## Архитектура (заглушка — дорабатывается в отчёте)

```
 [Браузер: HTML-форма]
        │  POST /api/orders  (JSON: buyer, product, amount)
        ▼
      [nginx]
        │  отдаёт static (форму)
        │  location /api/* → proxy_pass
        ▼
   [Flask: app.py]
        │
   ┌────┴─────────────────────────┬──────────────────────────────┐
   ▼                              ▼                               ▼
 [MySQL]                   [Kafka]                         [RabbitMQ]
 таблица orders            топик order-events               очередь order-notifications
 (order_id = PK)           (producer, key=order_id,         (текстовое уведомление)
                           3 партиции)
                           │            │
                           ▼            ▼
                    [consumer:      [consumer:
                    payment-group]   delivery-group]
```

### Компоненты
| Сервис | Назначение |
|---|---|
| MySQL | хранение заказов (таблица `orders`) |
| Kafka | событийная шина: топик `order-events` (3 партиции) |
| RabbitMQ | разовые текстовые уведомления в очередь `order-notifications` |
| Flask | приём `POST /api/orders`, запись в БД + публикация в шину/очередь |
| nginx | отдача HTML + прокси `/api/*` на Flask |
| Kafka UI / RabbitMQ Management | визуальная проверка работы |

### Команда
- **Участник 1** — Kafka: producer, 2 consumer group, топик, Kafka UI, nginx.
- **Участник 2** — RabbitMQ: publisher, воркер, management, Dockerfile, отчёт.

## Запуск
> Будет дополнен после всех шагов (см. план коммитов 1–14).

## Отчёт (заглушка)
- [ ] Схема архитектуры
- [ ] Почему одновременно Kafka и RabbitMQ
- [ ] Скриншоты Kafka UI и RabbitMQ Management
