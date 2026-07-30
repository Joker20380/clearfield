# CLEARFIELD — Event Intelligence Pipeline

CLEARFIELD — автономная система агрегации, кластеризации, нормализации и последующей генерации прикладного контента на основе новостных данных.

Изначальная задача системы — снижать информационный шум: превращать поток публикаций из разных источников в устойчивые события, пригодные для анализа и принятия решений.

Текущая production-роль проекта также включает downstream-пайплайн для медицинского сайта Dzagurov: от новостных событий до готовых медицинских публикаций, экспортируемых через публичный JSON-feed.

---

## Цель системы

* Склеивать разрозненные источники в единые события
* Удалять шум, заглушки, anti-adblock и loading-тексты
* Формировать короткие, стабильные summaries
* Создавать MedicalBrief на основе качественных событий
* Генерировать медицинские новости через локальный LLM-контур
* Автоматически публиковать прошедшие проверку материалы
* Экспортировать готовые новости в JSON-feed для внешнего сайта
* Работать автономно по cron-first модели
* Давать воспроизводимый и проверяемый результат

Ключевая формула:

```text
Raw data → Events → Briefs → Generated News → JSON Feed → Dzagurov
```

---

## Production-сценарий

На текущем этапе CLEARFIELD используется как генератор медицинских новостей для сайта Dzagurov.

Общий контур:

```text
CLEARFIELD / Timeweb
  ↓
генерация и экспорт JSON-feed
  ↓
Dzagurov / VPS
  ↓
импорт в main.News
  ↓
публикация на сайте
```

CLEARFIELD не пишет напрямую в базу Dzagurov. Связь между системами реализована через HTTPS pull:

1. CLEARFIELD формирует публичный JSON-feed.
2. Dzagurov скачивает feed.
3. Dzagurov импортирует новые элементы в `main.News`.
4. Дубликаты пропускаются по title.

---

## Архитектура пайплайна

Базовый event intelligence pipeline:

```text
Sources
  ↓
RawItem
  ↓
Article
  ↓
cluster_events
  ↓
Event + EventItem
  ↓
rebuild_event_summaries
  ↓
daily_brief
```

Медицинский downstream pipeline:

```text
Event
  ↓
rebuild_event_summaries
  ↓
create_medical_briefs
  ↓
MedicalBrief
  ↓
audit_medical_briefs
  ↓
generate_medical_news
  ↓
GeneratedMedicalNews
  ↓
auto_approve_medical_news
  ↓
export_medical_news_feed
  ↓
public JSON-feed
```

---

## Модели данных

### RawItem

Сырой элемент источника:

* title
* summary
* url
* published_at
* created_at
* item_hash

### Article

Результат извлечения контента:

* text
* extract_error

Может содержать мусор: anti-adblock, loading-заглушки, навигационные фрагменты и прочие нерелевантные блоки.

### Event

Сущность события:

* title
* summary
* region
* topic
* evidence_level
* cluster_key
* updated_at

### EventItem

Связь между `Event` и `RawItem`.

### MedicalBrief

Промежуточное медицинское задание для генерации новости.

Используется как контролируемый слой между событием и LLM.

Типовые поля:

* title
* angle
* target_keyword
* secondary_keywords
* facts
* source_urls
* audience
* region_text
* safety_notes
* status

Основные статусы:

```text
ready
used
rejected
error
```

### GeneratedMedicalNews

Готовая или промежуточная сгенерированная новость.

Основные статусы:

```text
draft
review
approved
published
transferred
error
```

В production-feed экспортируются новости со статусом:

```text
published
```

---

## Management commands

### cluster_events

Кластеризация `RawItem` в `Event` по SimHash.

```bash
python3 manage.py cluster_events --since-hours 24 --limit 2000
```

Свойства:

* идемпотентен
* не создаёт дубликатов
* результат `0 / 0` нормален при отсутствии новых данных

---

### rebuild_event_summaries

Пересборка `Event.summary` с очисткой шума.

```bash
python3 manage.py rebuild_event_summaries --hours 72
```

Источник summary, fallback-цепочка:

1. `Article.text`
2. `RawItem.summary`
3. `RawItem.title`

Особенности:

* удаляет anti-adblock и loading-шум
* не портит уже корректные summaries
* безопасен к частым запускам

---

### daily_brief

Генерация итогового Markdown-брифа.

```bash
python3 manage.py daily_brief --hours 72 --min-evidence 1
```

Особенности:

* чистит `Live:`-префиксы в заголовках
* выводит `(no summary)` при отсутствии текста
* SIGPIPE-safe, корректно работает с `head`, `tail`

---

### create_medical_briefs

Создание медицинских заданий на основе событий.

```bash
python3 manage.py create_medical_briefs \
  --hours 720 \
  --min-evidence 1 \
  --limit 10 \
  --min-score 5
```

Назначение:

* выбрать события, пригодные для медицинской новости
* сформировать структуру будущей публикации
* отделить сырой event-layer от LLM-generation layer

Если качественных событий нет, команда может завершиться сообщением:

```text
Нет качественных событий для MedicalBrief.
```

Это нормальный результат, а не ошибка.

---

### audit_medical_briefs

Проверка качества `MedicalBrief` перед генерацией.

```bash
python3 manage.py audit_medical_briefs --status ready
```

Назначение:

* отсеивать нерелевантные темы
* не тратить LLM-время на слабые новости
* повышать качество итогового feed

Примеры тем, которые желательно фильтровать жёстче:

* общие федеральные новости без локального контекста
* запись к врачу через MAX
* совещания без медицинской пользы для пациента
* новости не про Северную Осетию / Владикавказ
* технические или административные сообщения без полезного содержания

---

### generate_medical_news

Генерация медицинских новостей через LLM.

```bash
python3 manage.py generate_medical_news --limit 3
```

Команда берёт готовые `MedicalBrief` и создаёт `GeneratedMedicalNews` в статусе `review`.

Особенности:

* генерация одной новости может занимать 1–3 минуты
* `--limit 3` выбран как безопасный production-лимит
* LLM используется через локальный Ollama-контур, доступный через SSH tunnel

---

### auto_approve_medical_news

Автоматическая проверка и публикация сгенерированных новостей.

```bash
python3 manage.py auto_approve_medical_news \
  --status review \
  --limit 3 \
  --min-chars 1700 \
  --show-rejected
```

Назначение:

* проверяет минимальный объём
* проверяет локальный контекст
* пропускает слабые материалы
* переводит качественные новости в статус `published`

В production используется публикация именно в `published`, потому что JSON-feed экспортирует этот статус.

---

### export_medical_news_feed

Экспорт опубликованных медицинских новостей в публичный JSON-feed.

```bash
python3 manage.py export_medical_news_feed \
  --status published \
  --limit 20 \
  --show-content-size
```

Результат сохраняется в:

```text
`$CLEARFIELD_PUBLIC_DIR/generated-news/`
```

Пример имени файла:

```text
medical-news-feed-75945bcdc93c2d35861fa2a4e0ec5166.json
```

Feed предназначен для pull-импорта со стороны Dzagurov.

---

## LLM / Ollama

Генерация медицинских новостей использует локальный LLM-контур через Ollama.

Ollama не открывается наружу публично. Доступ организован через SSH tunnel.

Проверка туннеля:

```bash
./bin/ensure_ollama_tunnel.sh
```

Нормальный результат:

```text
Ollama tunnel already works.
```

Для cron важно, чтобы туннель поднимался без интерактивного ввода пароля.

---

## Production pipeline

Основной production-скрипт:

```text
bin/medical_news_pipeline.sh
```

Ручной запуск:

```bash
cd ~/clearfield/public_html/clearfield
source ../venv/bin/activate

./bin/medical_news_pipeline.sh | tee -a logs/medical_news_pipeline.log
```

Шаги pipeline:

```text
[1/7] Ensure Ollama tunnel
[2/7] Rebuild event summaries
[3/7] Create medical briefs
[4/7] Audit medical briefs
[5/7] Generate medical news via LLM
[6/7] Auto publish generated reviews
[7/7] Export public JSON feed
```

---

## Cron на Timeweb

На Timeweb shared-хостинге обычный `crontab` недоступен.

При попытке выполнить:

```bash
crontab -l
```

может быть сообщение:

```text
Редактирование cron-задач доступно только через Панель Управления Аккаунтом.
```

Поэтому cron для CLEARFIELD настраивается через панель Timeweb.

---

## Cron wrapper

Для запуска из панели Timeweb используется wrapper:

```text
bin/run_medical_news_pipeline_cron.sh
```

Он нужен, чтобы:

* выставить правильный рабочий каталог
* писать подробный лог
* видеть `USER`, `HOME`, `PWD`
* запускать production pipeline безопасно

Ручная проверка wrapper:

```bash
cd ~/clearfield/public_html/clearfield
source ../venv/bin/activate

./bin/run_medical_news_pipeline_cron.sh
tail -n 120 logs/medical_news_pipeline.cron.log
```

Признаки успешного запуска:

```text
Timeweb cron wrapper started
Medical news pipeline started
...
Medical news pipeline finished
Timeweb cron wrapper finished
```

---

## Команда для панели Timeweb

В панели Timeweb нужно указать команду:

```bash
/bin/bash -lc '/path/to/public_html/clearfield/bin/run_medical_news_pipeline_cron.sh'
```

Рекомендуемое расписание:

```cron
30 7 * * *
```

То есть ежедневно в 07:30.

На этапе тестирования можно временно поставить запуск каждые 5 минут:

```cron
*/5 * * * *
```

После проверки нужно вернуть ежедневное расписание.

---

## Интеграция с Dzagurov

CLEARFIELD только создаёт и экспортирует JSON-feed.

Импорт выполняется на стороне Dzagurov отдельным скриптом:

```text
/opt/apps/dzagurov/dzagurov/bin/pull_medical_news_from_clearfield.sh
```

Рекомендуемое расписание на Dzagurov:

```cron
50 7 * * *
```

То есть через 20 минут после запуска генерации на CLEARFIELD.

Общий порядок:

```text
07:30 CLEARFIELD генерирует новости и обновляет JSON-feed
07:50 Dzagurov скачивает feed и импортирует новые новости
```

---

## Логи

Основные логи CLEARFIELD:

```text
logs/medical_news_pipeline.log
logs/medical_news_pipeline.cron.log
logs/cron_cluster.log
logs/cron_rebuild.log
```

Просмотр:

```bash
tail -n 120 logs/medical_news_pipeline.cron.log
tail -n 120 logs/medical_news_pipeline.log
```

Логи позволяют отличить:

* отсутствие новых данных
* отбраковку событий по качеству
* ошибки LLM
* ошибки SSH tunnel
* ошибки export feed
* успешное завершение pipeline

---

## Проверка состояния очереди

Быстрая проверка `MedicalBrief` и `GeneratedMedicalNews`:

```bash
python3 manage.py shell -c "
from intel.models import MedicalBrief, GeneratedMedicalNews

print('MedicalBrief:')
for s in ['ready', 'used', 'rejected', 'error']:
    print(s, MedicalBrief.objects.filter(status=s).count())

print('GeneratedMedicalNews:')
for s in ['review', 'approved', 'published', 'transferred', 'error']:
    print(s, GeneratedMedicalNews.objects.filter(status=s).count())

for n in GeneratedMedicalNews.objects.order_by('-id')[:8]:
    print(n.id, n.status, n.title[:100])
"
```

---

## Проверка публичного feed

После экспорта нужно проверить, что JSON-feed доступен по HTTPS и возвращает HTTP 200.

Пример проверки:

```bash
curl -I https://YOUR_DOMAIN/generated-news/medical-news-feed-75945bcdc93c2d35861fa2a4e0ec5166.json
```

Ожидаемый результат:

```text
HTTP/2 200
```

Или:

```text
HTTP/1.1 200 OK
```

---

## Что не коммитить

В git нельзя добавлять:

```text
logs/
generated-news/
*.log
*.lock
.medical_news_feed_token
*.sqlite3
/tmp/
media/
```

Особенно важно не коммитить:

```text
generated-news/.medical_news_feed_token
```

---

## Рекомендуемые файлы для git

Для текущего production-контура в git должны быть зафиксированы:

```text
bin/medical_news_pipeline.sh
bin/run_medical_news_pipeline_cron.sh
bin/ensure_ollama_tunnel.sh
intel/management/commands/export_medical_news_feed.py
intel/management/commands/auto_approve_medical_news.py
intel/management/commands/create_medical_briefs.py
intel/management/commands/audit_medical_briefs.py
intel/management/commands/generate_medical_news.py
```

Перед commit:

```bash
git status --short
git diff --cached --name-only
```

---

## Устойчивость системы

* Нет постоянного Django worker
* Нет внешней очереди
* Нет прямой связи с базой Dzagurov
* Нет публичного Ollama endpoint
* Все шаги воспроизводимы вручную
* Cron-first архитектура
* Ошибки видны через обычные log-файлы
* Повторный импорт на Dzagurov безопасен: дубликаты пропускаются

---

## Ограничения

Текущие ограничения production-контура:

* качество новостей зависит от качества `MedicalBrief`
* `audit_medical_briefs` нужно постепенно ужесточать
* слабые темы могут доходить до LLM, если фильтр недостаточно строгий
* изображения для Dzagurov подбираются уже на стороне Dzagurov
* feed экспортирует последние опубликованные новости, а не полноценную очередь доставки
* статус `transferred` пока не используется как строгий delivery-механизм

---

## Ближайшие улучшения

Приоритетные задачи:

1. Ужесточить `audit_medical_briefs`
2. Фильтровать слабые административные темы до LLM
3. Улучшить локальный медицинский контекст для Дзагуров КДЛ
4. Добавить более точную тематическую классификацию
5. Поддержать source_id / external_id на стороне Dzagurov
6. Сделать delivery-статусы более строгими
7. Расширить контроль качества сгенерированных материалов
8. Добавить отчёт по каждому cron-прогону

---

## Философия

* Не скорость, а устойчивость
* Не количество, а связность
* Не поток, а события
* Не генерация ради генерации, а уменьшение неопределённости
* Не LLM вместо системы, а LLM как последний controlled step

CLEARFIELD — фундамент для аналитических систем, decision-support инструментов и downstream AI без шума.
