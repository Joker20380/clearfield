#!/usr/bin/env bash
set -Eeuo pipefail

BASE="$HOME/clearfield/public_html/clearfield"
VENV="$HOME/clearfield/public_html/venv/bin/activate"
LOG_DIR="$BASE/logs"
FEED_DIR="$HOME/clearfield/public_html/generated-news"

cd "$BASE"
mkdir -p "$LOG_DIR" "$FEED_DIR"

exec 9>"$LOG_DIR/medical_news_pipeline.lock"
if ! flock -n 9; then
  echo "$(date '+%F %T') Pipeline already running. Exit."
  exit 0
fi

source "$VENV"

echo "============================================================"
echo "$(date '+%F %T') Medical news pipeline started"
echo "============================================================"

echo "[1/10] Ensure Ollama tunnel"
./bin/ensure_llm_tunnel.sh

echo "[2/10] Ingest feeds"
python3 manage.py ingest_feeds \
  --limit 20 \
  --concurrency 3 \
  --max-items-per-source 30 \
  --allow-insecure-ssl

echo "[3/10] Cluster events"
python3 manage.py cluster_events \
  --limit 2000

echo "[3.5/10] Repair EventItem links"
python3 manage.py repair_eventitems \
  --limit 2000

echo "[3.7/10] Extract full article texts"
python3 manage.py extract_articles \
  --limit 80 \
  --concurrency 5 \
  --retries 2 \
  --timeout 40 \
  --allow-insecure-ssl

echo "[4/10] Rebuild event summaries"
python3 manage.py rebuild_event_summaries \
  --hours 720

echo "[5/10] Create medical briefs"
python3 manage.py create_medical_briefs \
  --hours 720 \
  --min-evidence 1 \
  --limit 10 \
  --min-score 0 \
  --min-summary-len 0 \
  --allow-weak \
  --show-rejected

echo "[6/10] Reject weak ready MedicalBrief items"
python3 manage.py shell -c "
import re

from intel.models import MedicalBrief

foreign = [
    'мурманск', 'мурманской области',
    'омск', 'омской области',
    'крым', 'ялта', 'севастопол',
    'запорож', 'энергодар',
]

weak = [
    'запись на прием через max',
    'запись на приём через max',
    'приложении max',
    'приложение max',
    'кредиторск',
    'задолженност',
    'совещание по снижению',
    'рабочее совещание',
    'человекоцентричная медицина',
    'фмба россии расширяет горизонты',

    # weak / irrelevant local-news noise
    'сво',
    'участникам сво',
    'росгвард',
    'военнослужащ',
    'отпуск expo',
    'выставке-форуме',
    'вольной борьбе',
    'соревнован',
    'спортсменов',
    'острове русский',
    'китай',
    'кндр',
    'корейскую народно-демократическую',
    'призвание становится судьбой',
    'совет общественных организаций',
    'общественных организаций',

    # definite non-medical local-news noise
    'мурат гассиев',
    'чемпионом мира по версии wba',
    'осетинском театре',
    'актерского спецкурса',
    'спектакль «черная бурка»',
    'летняя погода',
    'внимание! перекрытие',
    'расчистка русел рек',
    'по ту сторону спортивных новостей',
    'международном форуме по безопасности',
]

def normalize_filter_text(value):
    return ' '.join(
        (value or '')
        .lower()
        .replace('ё', 'е')
        .split()
    )


def contains_term(text, term):
    term = normalize_filter_text(term)

    if not term:
        return False

    # Короткие маркеры вроде СВО должны совпадать только как отдельное слово.
    if re.fullmatch(r'[0-9a-zа-я]+', term) and len(term) <= 3:
        pattern = (
            rf'(?<![0-9a-zа-я])'
            rf'{re.escape(term)}'
            rf'(?![0-9a-zа-я])'
        )
        return re.search(pattern, text) is not None

    return term in text


bad = []

for b in MedicalBrief.objects.filter(status='ready'):
    # Шумовую тематику проверяем только по заголовку и ракурсу.
    # Случайная фраза внутри facts не должна отклонять медицинскую новость.
    headline_text = normalize_filter_text(' '.join([
        b.title or '',
        b.angle or '',
    ]))

    # Регион проверяем также по явно сохранённому region_text.
    foreign_text = normalize_filter_text(' '.join([
        b.title or '',
        b.angle or '',
        b.region_text or '',
    ]))

    foreign_hits = [
        term for term in foreign
        if contains_term(foreign_text, term)
    ]

    weak_hits = [
        term for term in weak
        if contains_term(headline_text, term)
    ]

    if foreign_hits or weak_hits:
        bad.append(b.id)
        print(
            'REJECT weak brief:',
            b.id,
            'foreign=',
            foreign_hits,
            'weak=',
            weak_hits,
            '|',
            b.title[:120],
        )

MedicalBrief.objects.filter(id__in=bad).update(status='rejected')
print('Rejected weak briefs:', len(bad))
"

echo "[7/10] Audit medical briefs"
python3 manage.py audit_medical_briefs \
  --status ready

echo "[8/10] Generate medical news via LLM"
python3 manage.py generate_medical_news \
  --limit 3

echo "[9/10] Auto publish generated reviews"
python3 manage.py auto_approve_medical_news \
  --status review \
  --limit 10 \
  --min-chars 1700 \
  --show-rejected

echo "[9.5/10] Reject weak GeneratedMedicalNews before export"
python3 manage.py shell -c "
from django.db import models
from intel.models import GeneratedMedicalNews

bad_markers = [
    'мурманск',
    'мурманской области',
    'омск',
    'омской области',
    'крым',
    'ялта',
    'севастопол',
    'запорож',
    'энергодар',
    'кредиторск',
    'задолженност',
    'совещание по снижению',
    'рабочее совещание',
    'запись на прием через max',
    'запись на приём через max',
    'приложении max',
    'приложение max',
]

text_fields = [
    f.name for f in GeneratedMedicalNews._meta.fields
    if isinstance(f, (models.CharField, models.TextField))
]

bad = []

for n in GeneratedMedicalNews.objects.filter(status__in=['review', 'published']).order_by('-id')[:30]:
    text = ' '.join(str(getattr(n, f, '') or '') for f in text_fields).lower().replace('ё', 'е')

    if any(marker in text for marker in bad_markers):
        bad.append(n.id)
        print('REJECT weak generated news:', n.id, '|', n.status, '|', n.title[:120])

GeneratedMedicalNews.objects.filter(id__in=bad).update(status='rejected')
print('Rejected weak generated news:', len(bad))
"

echo "[10/10] Export public JSON feed"
python3 manage.py export_medical_news_feed \
  --status published \
  --limit 20 \
  --show-content-size

chmod 755 "$FEED_DIR"
chmod 644 "$FEED_DIR"/medical-news-feed-*.json 2>/dev/null || true
chmod 600 "$FEED_DIR"/.medical_news_feed_token 2>/dev/null || true

echo "============================================================"
echo "$(date '+%F %T') Medical news pipeline finished"
echo "============================================================"
