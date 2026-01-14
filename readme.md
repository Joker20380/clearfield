# CLEARFIELD — Event Intelligence Pipeline

CLEARFIELD — автономная система агрегации, кластеризации и нормализации новостных данных,
ориентированная на снижение информационного шума и формирование устойчивых событий.

Система не строит новостную ленту и не пересказывает источники.
Она превращает поток публикаций в события, пригодные для анализа и принятия решений.

---

## Цель системы

- Склеивать разрозненные источники в единые события
- Удалять шум, заглушки и anti-adblock тексты
- Формировать короткие, стабильные summaries
- Работать автономно (cron-first)
- Давать воспроизводимый и проверяемый результат

Ключевая формула:

Raw data → Events → Clarity

---

## Архитектура пайплайна

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
daily_brief (Markdown)

---

## Модели данных

### RawItem
Сырой элемент источника:
- title
- summary
- url
- published_at
- created_at
- item_hash

### Article
Результат извлечения контента:
- text
- extract_error

Может содержать мусор (anti-adblock, from loading и т.п.).

### Event
Сущность события:
- title
- summary
- region
- topic
- evidence_level
- cluster_key
- updated_at

### EventItem
Связь Event ↔ RawItem (многие-к-одному).

---

## Management commands

### cluster_events
Кластеризация RawItem в Event по SimHash.

python manage.py cluster_events --since-hours 24 --limit 2000

Свойства:
- идемпотентен
- не создаёт дубликатов
- 0 / 0 — нормальный результат при отсутствии новых данных

---

### rebuild_event_summaries
Пересборка Event.summary с очисткой шума.

python manage.py rebuild_event_summaries --hours 72

Источник summary (fallback-цепочка):
1. Article.text
2. RawItem.summary
3. RawItem.title

Особенности:
- удаляет anti-adblock и loading-шум
- не портит уже корректные summaries
- безопасен к частым запускам

---

### daily_brief
Генерация итогового Markdown-брифа.

python manage.py daily_brief --hours 72 --min-evidence 1

Особенности:
- чистит Live:-префиксы в заголовках
- выводит (no summary) при отсутствии текста
- SIGPIPE-safe (корректно работает с head, tail)

---

## Автоматизация (cron)

Система рассчитана на автономную работу через cron.

### Скрипты

public_html/cron/cluster_24h.sh  
public_html/cron/rebuild_72h.sh

### cluster_24h.sh
Запуск кластеризации новых данных.
Рекомендуемая периодичность: каждые 30 минут.

### rebuild_72h.sh
Пересборка summaries за последние 72 часа.
Рекомендуемая периодичность: 1–2 раза в час.

Все скрипты пишут логи в:

public_html/logs/

---

## Логи и наблюдаемость

Просмотр логов:

tail -n 100 logs/cron_cluster.log  
tail -n 100 logs/cron_rebuild.log

Логи позволяют отличить:
- отсутствие новых данных
- фильтрацию по качеству
- реальные ошибки

---

## Устойчивость системы

- Нет демонов
- Нет очередей
- Нет внешних API
- Все шаги воспроизводимы вручную
- Минимальные требования к инфраструктуре

---

## Философия

- Не скорость, а устойчивость
- Не количество, а связность
- Не поток, а события

CLEARFIELD — фундамент для аналитических систем,
decision-support инструментов и downstream AI без шума.