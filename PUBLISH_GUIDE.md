# PUBLISH_GUIDE.md — Пошаговая инструкция публикации портфолио на GitHub

> Этот документ — пошаговое руководство: как выложить `data-platform/` на GitHub и оформить профиль так, чтобы рекрутер увидел все навыки за 30 секунд.

**Время выполнения:** ~20 минут.

---

## Предварительные требования

- Аккаунт на [github.com](https://github.com) (если нет — зарегистрируйтесь, бесплатный план sufficient)
- Git установлен локально (`git --version`, минимальная версия 2.40+)
- SSH-ключ добавлен в GitHub ([guide](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)) ИЛИ используйте HTTPS с Personal Access Token
- Python 3.10+ (для локальных проверок; опционально)

---

## Шаг 1. Создайте новый репозиторий на GitHub

1. Откройте [github.com/new](https://github.com/new)
2. Заполните форму:
   - **Owner:** ваш username (например, `dobrycode`)
   - **Repository name:** `data-platform`
   - **Description:** `Data-platform монорепозиторий: Airflow + dbt + Spark + Data Vault 2.0 + Iceberg + DQ + Cube + k8s + CI/CD`
   - **Visibility:** ✅ **Public** (обязательно — рекрутер должен видеть без приглашения)
   - **Initialize this repository:** ❌ НЕ ставьте галочки (Add README, .gitignore, license) — мы загрузим свои
3. Нажмите **Create repository**
4. Скопируйте URL: `https://github.com/<username>/data-platform.git`

---

## Шаг 2. Инициализируйте git локально

Откройте терминал, перейдите в папку портфолио:

```bash
cd /path/to/data-platform
```

Инициализируйте git и сделайте первый коммит:

```bash
git init -b main
git add .
git status                              # проверьте, что все 40 файлов staged
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
- DataHub ingestion recipes"
```

---

## Шаг 3. Привяжите remote и сделайте push

```bash
git remote add origin https://github.com/<username>/data-platform.git
# или SSH:
# git remote add origin git@github.com:<username>/data-platform.git

git push -u origin main
```

Если GitHub попросит авторизацию (HTTPS) — используйте Personal Access Token (с правом `repo`):
- Создайте токен: [github.com/settings/tokens](https://github.com/settings/tokens) → Generate new token (classic) → галочка `repo`
- При push введите username и токен вместо пароля

---

## Шаг 4. Проверьте, что репозиторий открылся публично

1. Откройте `https://github.com/<username>/data-platform` в режиме инкогнито
2. Проверьте, что README.md рендерится (с таблицей 18 навыков, ASCII-архитектурой, деревом файлов)
3. Если README не отображается как главная страница:
   - Settings → General → Default branch → убедитесь, что `main`
   - README.md должен быть в корне репозитория

---

## Шаг 5. Настройте GitHub-профиль

### 5.1. Bio и аватар
- [github.com/settings/profile](https://github.com/settings/profile)
- **Name:** Червак Алексей Петрович (или Алексей Червак)
- **Bio:** `Senior Data Engineer | Spark · Airflow · dbt · Data Vault 2.0 · Iceberg · k8s | 5+ years in Sber / MTS / EY`
- **Company:** ИП Червак (или оставьте пустым)
- **Location:** Москва, Россия
- **Website:** ссылка на резюме PDF (если зальёте в releases)
- **Pronouns:** опционально

### 5.2. Pinned repositories
На главной странице профиля (`github.com/<username>`):
- Нажмите **Customize your pins**
- Закрепите `data-platform` первым (отображается сверху)
- Дополнительно — pet-проекты (e-commerce, CV-coffee-roast и т.д.)

### 5.3. Profile README (опционально, очень рекомендуется)

Создайте **отдельный** репозиторий с именем = вашему username:
```bash
mkdir <username>
cd <username>
git init -b main
```

Создайте `README.md`:
```markdown
# Червак Алексей Петрович — Senior Data Engineer

<img src="https://img.shields.io/badge/Python-3.11-blue"> <img src="https://img.shields.io/badge/Spark-3.5-orange"> <img src="https://img.shields.io/badge/Airflow-2.9-green"> <img src="https://img.shields.io/badge/dbt-1.7-purple">

> Data Engineer с 5+ годами опыта в банках и телекоме (Сбер, МТС, EY).
> Специализация: data-платформы под ключ, ETL/ELT, Data Vault 2.0, streaming.

## Портфолио
- **[data-platform](https://github.com/<username>/data-platform)** — data-platform монорепо «под ключ»
  - Airflow DAGs · dbt models · Spark jobs · Data Vault 2.0 · Iceberg · DQ · Cube · k8s · CI/CD

## Контакты
- Telegram: [@dobrycode](https://t.me/dobrycode)
- Email: chervak_2016@mail.ru
- Phone: +7 (993) 622-81-82
- Moscow, Russia

## Текущий статус
Открыт к предложениям на Senior Data Engineer / Data Platform Engineer (РФ + international).
Подробная матрица навыков с уровнями и версиями — по запросу.
```

```bash
git add README.md
git commit -m "docs: profile README"
git remote add origin https://github.com/<username>/<username>.git
git push -u origin main
```

Этот README будет отображаться прямо на главной странице профиля.

---

## Шаг 6. Создайте Release v1.0.0 (тег релиза)

```bash
cd /path/to/data-platform
git tag -a v1.0.0 -m "Release v1.0.0 — data-platform portfolio

Full data engineering stack implemented as working code.
Skills matrix: 25 Pro / 34 Advanced / 3 Base.
"

git push origin v1.0.0
```

На GitHub:
- Откройте репозиторий → **Releases** → **Draft a new release**
- Choose tag → `v1.0.0`
- Title: `Portfolio v1.0.0 — Senior Data Engineer`
- Description:
  ```markdown
  ## Что нового
  
  Первый публичный release. Все ключевые навыки покрыты:
  
  - Airflow 2.9 (TaskFlow API, dynamic DAGs, KubernetesPodOperator, custom hook/operator)
  - dbt 1.7 (staging/intermediate/marts, incremental, snapshots, custom macros/tests, Slim CI)
  - Spark 3.5 (skew salting, broadcast-join, AQE, Structured Streaming)
  - Data Vault 2.0 (Hub/Link/Satellite, SHA-256, SD2)
  - Apache Iceberg 1.4 (MERGE INTO, time travel)
  - Great Expectations 0.18
  - Cube-core 0.34
  - .gitlab-ci.yml full pipeline
  - Helm chart for Airflow on k8s
  - Multi-stage Dockerfile <350 MB
  - dbt data contracts
  - Trino 428 / StarRocks 3.2 docs
  
  **Author:** Червак Алексей Петрович (@dobrycode)
  ```
- **Attach files:** прикрепите PDF-резюме и skills_matrix.xlsx (drag-and-drop)
- Publish release

---

## Шаг 7. Добавьте темы (topics) для discoverability

На странице репозитория (`github.com/<username>/data-platform`):
- Справа от About → ⚙️ → **Topics**
- Добавьте: `data-engineering` `apache-spark` `airflow` `dbt` `data-vault` `iceberg` `kubernetes` `apache-kafka` `great-expectations` `data-platform` `etl` `big-data` `python`
- Description: `Data-platform монорепозиторий: Airflow + dbt + Spark + Data Vault 2.0 + Iceberg + DQ + Cube + k8s + CI/CD`
- ✅ Releases, Packages, Environments — оставить

---

## Шаг 8. Включите GitHub Pages (опционально, для dbt docs)

Если хотите опубликовать dbt docs как статический сайт:
1. Settings → Pages → Source: **GitHub Actions**
2. Создайте `.github/workflows/dbt_docs.yml` в репозитории:
   ```yaml
   name: Publish dbt docs
   on:
     push:
       branches: [main]
       paths: [dbt/**]
   jobs:
     build:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with: { python-version: '3.11' }
         - run: pip install dbt-core==1.7 dbt-postgres==1.7
         - run: cd dbt && dbt docs generate --target ci
         - uses: peaceiris/actions-gh-pages@v3
           with:
             github_token: ${{ secrets.GITHUB_TOKEN }}
             publish_dir: dbt/target/
   ```
3. URL: `https://<username>.github.io/data-platform/` — будет показывать lineage DAG ваших dbt-моделей

---

## Шаг 9. Финальная проверка перед отправкой отклика

Откройте чек-лист:

- [ ] Репозиторий публичный (проверено в режиме инкогнито)
- [ ] README.md рендерится как главная страница (с таблицей 18 навыков)
- [ ] Все 40 файлов загружены (проверьте: `git ls-files | wc -l` ≈ 40)
- [ ] Создан тег `v1.0.0` (виден в Releases)
- [ ] В Releases прикреплены: PDF-резюме + skills_matrix.xlsx
- [ ] Topics добавлены
- [ ] GitHub-профиль: bio заполнен, `data-platform` закреплён в Pinned
- [ ] (Опционально) Profile README с именем = username создан
- [ ] (Опционально) GitHub Pages с dbt docs включён

---

## Шаг 10. Отправка отклика

Скопируйте универсальный шаблон отклика из чата (подготовлен отдельно как готовый текст) и отправьте рекрутеру / в форму заявки, заменив `[Компания]` и `[Позиция]` на конкретные значения.

---

## FAQ

**Q: Почему GitHub, а не GitLab?**
A: Большинство работодателей принимают оба. GitHub выбрали, потому что: (1) recruiter'ам привычнее; (2) GitHub Pages для dbt docs; (3) больший охват в РФ-сообществе. CI/CD-навык продемонстрирован `.gitlab-ci.yml` в репозитории — это доказывает, что умеем работать и с GitLab.

**Q: Что если репозиторий уже залит с другим именем?**
A: В README и отклике поменяйте `github.com/dobrycode/data-platform` на ваш фактический URL. Используйте:
```bash
find . -type f -exec sed -i 's|github.com/dobrycode|github.com/<your_username>|g' {} +
```

**Q: Нужно ли делать репозиторий приватным?**
A: Нет. Работодатели просят публичные ссылки на GitHub/GitLab. Публичный профиль — это часть оценки технического уровня.

**Q: Что если нет GitHub-аккаунта?**
A: Создайте — это бесплатно и займёт 5 минут.

**Q: Можно ли добавить pet-проекты?**
A: Да, очень рекомендуется. Создайте отдельные репозитории и закрепите их в Pinned. Особенно ценятся: e-commerce платформа, ML-uplift, CV-проекты.

---

*Подготовлено как portfolio для отклика на позиции Senior Data Engineer / Data Platform Engineer.*
