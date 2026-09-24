# Памятка для второго участника (TEAMMATE)

> Краткая инструкция «что делать и какие команды». Полный контекст — в `README.md`.
> Работаем по **правилу 2 человек**: каждый коммитит/пушит только СВОИ файлы.

## Стек в одной строке

HTML-форма (`web/`) → **nginx** (`:8080`) → **Flask** (`app.py`) → **MySQL** + **Kafka** (`order-events`) + **RabbitMQ** (`order-notifications`);
поверх Kafka — две независимые consumer-группы `payment-group` и `delivery-group`; RabbitMQ-воркер «доставляет» разовые уведомления.

## Быстрый старт

```bash
docker compose up -d --build      # собрать и поднять весь стек
docker compose ps                 # все Up; mysql/kafka/rabbitmq/nginx — healthy
bash scripts/e2e_test.sh          # автопрогон (ожидается ALL GREEN ✅)
```

Открыть в браузере:
- Форма заказа:  `http://localhost:8080/`
- Kafka UI:      `http://localhost:8002/`   (кластер `lab-cluster`, топик `order-events`)
- RabbitMQ UI:   `http://localhost:15672/`   (логин/пароль `guest`/`guest`)

## Что где (зона P2 — RabbitMQ)

| Файл / сервис | Кто отвечает | Что делает |
|---|---|---|
| `python/rabbit_worker.py` | P2 | воркер очереди `order-notifications` |
| `python/Dockerfile` | P2 | общий образ `lab-python:py311` |
| `python/app.py` (RabbitMQ-часть) | P1+P2 | publisher в очередь |
| `rabbitmq/` (конфиги) | P2 | пока пусто, настройки живут в compose |
| `python/kafka_consumer_delivery.py` | P1 | вторая consumer-группа `delivery-group` |
| `nginx/`, `web/` | P1 | HTML-форма + прокси `/api/*` |

## Частые команды

```bash
docker compose up -d --build      # собрать и поднять
docker compose ps                 # статус всех сервисов
docker compose logs -f <svc>      # логи (flask, rabbitmq-worker, kafka-consumer-payment, kafka-consumer-delivery)
docker compose exec -T mysql mysql -uapp -papppass orders_db -e "SELECT * FROM orders"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker compose down               # остановить (объёмы mysql_data/kafka_data сохраняются)
docker compose down -v           # остановить + удалить данные
```

## Как проверить конвейер (E2E)

`bash scripts/e2e_test.sh` — делает 3 POST-заказа через nginx и проверяет всё:
MySQL (+3 строки) · Kafka: обе группы есть и lag=0 · логи payment/delivery · RabbitMQ-воркер доставил 3 уведомления.

## Если host-порты заняты

Заняты на хосте: `3306 / 5000 / 29092 / 15672` — перенеси **только host-часть** в `ports:` в `docker-compose.yml`
(например `3306→3307`, `5000→5001`, `29092→29093`, `15672→15673`). Внутренняя связь сервисов по имени **не меняется**.

## Правила 2 человек

- Коммить и пушь **только свои** файлы.
- Перед коммитом: `git pull` (учесть изменения другого), при конфликтах — разрешить.
- После каждого пуша: `bash scripts/e2e_test.sh` → **ALL GREEN**.

## Чеклист перед коммитом

- [ ] `docker compose config -q` — compose валиден
- [ ] `docker compose up -d --build` — стек поднялся
- [ ] `bash scripts/e2e_test.sh` → **ALL GREEN**
- [ ] в `git add` попадают **только мои** файлы

> Скриншоты/артефакты для отчёта складывай в каталог `report/` (пока только `.gitkeep`).
