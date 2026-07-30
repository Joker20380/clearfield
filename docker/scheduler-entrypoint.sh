#!/usr/bin/env bash
set -Eeuo pipefail

export -p >/run/clearfield-container.env
chown root:appuser /run/clearfield-container.env
chmod 0640 /run/clearfield-container.env

cat >/etc/cron.d/clearfield <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
TZ=Europe/Moscow

30 7 * * * appuser . /run/clearfield-container.env; /bin/bash /opt/apps/clearfield/clearfield/bin/run_medical_news_pipeline_cron.sh
5 * * * * appuser . /run/clearfield-container.env; REGIONAL_DIGEST_SCHEDULED=1 /bin/bash /opt/apps/clearfield/clearfield/bin/run_regional_digest_pipeline_cron.sh
15 * * * * appuser . /run/clearfield-container.env; REGIONAL_DIGEST_SCHEDULED=1 /bin/bash /opt/apps/clearfield/clearfield/bin/run_russia_medical_digest_pipeline_cron.sh
30 8 * * * appuser . /run/clearfield-container.env; /bin/bash /opt/apps/clearfield/clearfield/bin/run_automotive_news_pipeline_cron.sh
CRON

chmod 0644 /etc/cron.d/clearfield
exec cron -f
