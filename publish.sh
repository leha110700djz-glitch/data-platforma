#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# publish.sh — выложить data-platform портфолио на GitHub
# ═══════════════════════════════════════════════════════════════════════════
#
# Запускать на Linux / macOS / Git Bash на Windows.
#
# Что делает скрипт:
#   1. Проверяет, что git установлен
#   2. Копирует портфолио в новую рабочую папку (чтобы не портить исходник)
#   3. Инициализирует git-репозиторий в этой папке
#   4. Делает первый commit
#   5. Привязывает remote и push'ит на GitHub
#   6. Создаёт тег v1.0.0 и push'ит тег
#
# ЧТО НУЖНО СДЕЛАТЬ ВРУЧНУЮ ДО ЗАПУСКА:
#   1. Создать репозиторий на github.com/new
#      - Repository name: data-platform
#      - Visibility: Public
#      - НЕ ставить галочки "Initialize this repository"
#   2. Подставить свой GitHub username в переменную GH_USER ниже
#   3. Запустить: bash publish.sh
# ═══════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── НАСТРОЙКИ (отредактируйте под себя) ───────────────────────────────────
GH_USER="dobrycode"                       # ваш GitHub username
REPO_NAME="data-platform"                 # имя репозитория на GitHub
SOURCE_DIR="/home/z/my-project/download/portfolio/data-platform"
WORK_DIR="$HOME/${REPO_NAME}-publish"     # рабочая папка (будет создана)
# ───────────────────────────────────────────────────────────────────────────

# Цвета вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()    { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()     { echo -e "${GREEN}[OK]${NC} $*"; }
warn()   { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()   { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  publish.sh — выложить data-platform на GitHub"
echo "  GitHub user: ${GH_USER}"
echo "  Repo name:   ${REPO_NAME}"
echo "════════════════════════════════════════════════════════════════"
echo ""

# ─── Шаг 0: проверки ───────────────────────────────────────────────────────
log "Шаг 0/6: проверки окружения"

command -v git >/dev/null 2>&1 || fail "git не установлен. Установите: https://git-scm.com/downloads"
ok "git установлен: $(git --version)"

[ -d "${SOURCE_DIR}" ] || fail "Папка источника не найдена: ${SOURCE_DIR}"
ok "источник: ${SOURCE_DIR} ($(find ${SOURCE_DIR} -type f | wc -l) файлов)"

# Проверяем, что пользователь отредактировал GH_USER
if [ "${GH_USER}" = "dobrycode" ]; then
    warn "GH_USER='dobrycode' — оставлено значение по умолчанию."
    warn "Если это НЕ ваш GitHub username, прервите скрипт (Ctrl+C) и отредактируйте переменную GH_USER."
    warn "Через 5 секунд продолжаем..."
    sleep 5
fi

echo ""
log "Шаг 1/6: подготовка рабочей папки"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
cp -r "${SOURCE_DIR}/." "${WORK_DIR}/"
cd "${WORK_DIR}"
ok "рабочая папка: ${WORK_DIR}"
ok "файлов скопировано: $(find . -type f | wc -l)"

echo ""
log "Шаг 2/6: git init + настройка пользователя (если ещё не настроен)"
git init -b main

# Настройка git user, если ещё не задана
if ! git config user.name | grep -q .; then
    git config --global user.name "Червак Алексей Петрович"
    ok "установлен user.name: Червак Алексей Петрович"
else
    ok "user.name уже установлен: $(git config user.name)"
fi
if ! git config user.email | grep -q .; then
    git config --global user.email "chervak_2016@mail.ru"
    ok "установлен user.email: chervak_2016@mail.ru"
else
    ok "user.email уже установлен: $(git config user.email)"
fi

echo ""
log "Шаг 3/6: git add + первый commit"
git add .
echo "  файлов staged: $(git diff --cached --numstat | wc -l)"
git commit -m "feat: initial commit — data-platform monorepo

Implements full data engineering stack:
- Airflow 2.9 (TaskFlow API, dynamic DAGs, KubernetesPodOperator, custom hook/operator)
- dbt 1.7 (staging/intermediate/marts, incremental, snapshots SCD2, custom macros)
- Spark 3.5 (optimization, salting for skew, Structured Streaming)
- Data Vault 2.0 (Hub/Link/Satellite, SHA-256 hash keys, SD2)
- Apache Iceberg 1.4 (MERGE INTO, time travel, partition evolution)
- Great Expectations 0.18 (suite + checkpoint)
- Cube-core 0.34 (measures, dimensions, pre-aggregations)
- .gitlab-ci.yml (lint/test/dbt docs/build/deploy with MR pipelines)
- Helm chart for Airflow on k8s
- Multi-stage Dockerfile (image <350 MB)
- dbt data contracts (schema + SLA + SLO + owner)
- S3 lifecycle automation (boto3)
- Trino/StarRocks docs (federation + POC)
- DataHub ingestion recipes
"
ok "commit создан: $(git log --oneline -1)"

echo ""
log "Шаг 4/6: привязка remote к GitHub"
REMOTE_URL="https://github.com/${GH_USER}/${REPO_NAME}.git"
git remote add origin "${REMOTE_URL}" || warn "remote уже существует, обновляем URL"
git remote set-url origin "${REMOTE_URL}"
ok "remote установлен: ${REMOTE_URL}"

echo ""
log "Шаг 5/6: git push"
echo "  Если попросит логин и пароль:"
echo "    Username: ваш GitHub username (${GH_USER})"
echo "    Password: Personal Access Token (НЕ пароль от аккаунта!)"
echo "             создать токен: https://github.com/settings/tokens"
echo "             галочка: ☑ repo"
echo ""
git push -u origin main
ok "push завершён успешно"

echo ""
log "Шаг 6/6: создание тега v1.0.0"
git tag -a v1.0.0 -m "Release v1.0.0 — data-platform portfolio

Full data engineering stack implemented as working code.
Skills matrix: 25 Pro / 34 Advanced / 3 Base.
"
git push origin v1.0.0
ok "тег v1.0.0 запушен"

echo ""
echo "════════════════════════════════════════════════════════════════"
echo -e "${GREEN}  ГОТОВО! Репозиторий опубликован:${NC}"
echo ""
echo "  ${CYAN}https://github.com/${GH_USER}/${REPO_NAME}${NC}"
echo ""
echo "  Файл README.md отобразится как главная страница."
echo "  Тег v1.0.0 виден в Releases (создайте Release вручную на GitHub)"
echo ""
echo "  СЛЕДУЮЩИЕ ШАГИ (на github.com):"
echo "  1. Откройте репозиторий → About → ⚙️ → добавьте topics:"
echo "     data-engineering, apache-spark, airflow, dbt, data-vault,"
echo "     iceberg, kubernetes, kafka, great-expectations, etl, big-data"
echo ""
echo "  2. Создайте Release v1.0.0:"
echo "     Releases → Draft a new release → Choose tag: v1.0.0"
echo "     Title: Portfolio v1.0.0 — Senior Data Engineer"
echo "     Прикрепите файлы (drag&drop):"
echo "       - Червак_Алексей_Senior_Data_Engineer.pdf"
echo "       - skills_matrix.xlsx"
echo "       - Cover_Letter_Universal_Червак_Алексей.pdf"
echo ""
echo "  3. Настройте профиль:"
echo "     github.com/settings/profile → Bio:"
echo "       Senior Data Engineer | Spark · Airflow · dbt · Data Vault 2.0 · Iceberg · k8s | 5+ yrs Sber/MTS/EY"
echo ""
echo "  4. Pin репозиторий на главной странице профиля"
echo "     github.com/${GH_USER} → Customize your pins → ☑ data-platform"
echo "════════════════════════════════════════════════════════════════"
