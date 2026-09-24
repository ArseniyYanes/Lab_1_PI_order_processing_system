# Лабораторная работа — Система обработки заказов

Командная работа (2 человека). Веб-страница с формой заказа → Python (Flask) → MySQL + Kafka + RabbitMQ. Всё поднимается через `docker-compose.yml`; наружу выводится `nginx`, который отдаёт HTML-форму и проксирует `/api/*` на Flask-обработчик.

## Архитектура

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
| Kafka consumer (payment-group) | «оплата»: читает `order-events`, логирует обработку платежа |
| Kafka consumer (delivery-group) | «доставка»: независимо читает тот же топик (своя consumer-группа) |
| RabbitMQ worker | читает durable-очередь `order-notifications`, «доставляет» уведомления |
| Kafka UI / RabbitMQ Management | визуальная проверка работы |

### Команда
- **Участник 1** — Kafka: producer, 2 consumer group, топик, Kafka UI, nginx.
- **Участник 2** — RabbitMQ: publisher, воркер, management, Dockerfile, отчёт.

## Запуск

Требуется: Docker + Docker Compose v2 (подкоманда `docker compose`).

### 1. Поднять весь стек

```bash
docker compose up -d --build
```

Первый запуск дольше: сборка образа `lab-python:py311` и прохождение healthcheck'ов
(Кafka ~60 с, MySQL ~40 с). Убедиться, что сервисы поднялись:

```bash
docker compose ps      # все Up; mysql/kafka/rabbitmq/nginx — healthy
```

### 2. Открыть форму и UI

| Что | Адрес |
|---|---|
| Форма заказа (nginx) | http://localhost:8080/ |
| API (Flask через nginx) | `POST http://localhost:8080/api/orders` |
| Kafka UI | http://localhost:8002/ (кластер `lab-cluster`, топик `order-events`) |
| RabbitMQ Management | http://localhost:15672/ (`guest`/`guest`, очередь `order-notifications`) |

### 3. Проверить работу

- **Через браузер**: открыть форму, заполнить и нажать «Оформить заказ» → `HTTP 201` с `order_id`.
- **Скрипт** (автоматически 3 заказа + проверка всех компонентов):

```bash
bash scripts/e2e_test.sh
```

### Остановить

```bash
docker compose down       # удалить контейнеры (объёмы mysql_data/kafka_data сохраняются)
docker compose down -v    # + удалить объёмы (БД и данные Kafka — заново)
```

### Если host-порты заняты

По умолчанию наружу выводятся: `8080` (nginx), `5000` (flask), `3306` (mysql),
`29092` (kafka external), `8002` (Kafka UI), `5672`/`15672` (RabbitMQ AMQP/UI).
Если какой-то занят — поменяйте только **host-часть** в `ports:` (внутренний адрес и
маршрутизацию по имени сервиса менять не нужно).


## Отчёт

### Схема архитектуры
См. блок «Архитектура» выше: HTML-форма → nginx → Flask → (MySQL + Kafka + RabbitMQ);
поверх Kafka — две независимые consumer-группы (`payment-group`, `delivery-group`).

### Почему одновременно Kafka и RabbitMQ
- **Kafka** — журнал событий (event log): топик `order-events` хранит историю заказов,
  к каждой записи привязан `order_id` (ключ) → 3 партиции. Разные «потребители»
  (оплата и доставка) читают поток независимо, каждый со своим смещением (group offset).
  Это даёт повторное чтение, масштабирование и отказоустойчивость: «оплата» и «доставка»
  не мешают друг другу.
- **RabbitMQ** — доставка **разовых** одноразовых уведомлений (e-mail/SMS) в
  durable-очередь `order-notifications`: сообщение ставится в очередь и исчезает после
  доставки одним воркером (ack после доставки, `prefetch=1`). Для «одного раза на каждого
  получателя» это проще и дешевле, чем Kafka.

**Вывод:** для событий, которые должны остаться и быть прочитанными несколькими
независимыми потребителями — Kafka; для «разового задания» одному исполнителю — RabbitMQ.

### Как проверить работу (что пощупать)
1. `bash scripts/e2e_test.sh` — автопрогон: 3 заказа + проверки MySQL/Kafka/RabbitMQ (ожидается `ALL GREEN ✅`).
2. **Kafka UI** (`http://localhost:8002`): Topics → `order-events` (3 партиции); Consumer Groups → `payment-group`, `delivery-group` (lag = 0).
3. **RabbitMQ Management** (`http://localhost:15672`, `guest`/`guest`): Queues → `order-notifications` (durable).
4. Логи консьюмеров/воркера:

   ```bash
   docker compose logs -f kafka-consumer-payment kafka-consumer-delivery rabbitmq-worker
   ```

Скриншоты Kafka UI и RabbitMQ Management складывайте в каталог `report/`.
