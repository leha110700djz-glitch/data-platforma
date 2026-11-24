#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# check_env.sh — проверить что окружение готово к публикации на GitHub
# ═══════════════════════════════════════════════════════════════════════════
# Запускать ДО publish.sh, чтобы понять что ещё нужно настроить.

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok()    { echo -e "  ${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "  ${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "  ${RED}[FAIL]${NC} $*"; }

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  Проверка окружения для публикации на GitHub"
echo "════════════════════════════════════════════════════════════════"
echo ""

# 1. git
echo "1. Git:"
if command -v git >/dev/null 2>&1; then
    ok "установлен: $(git --version)"
else
    fail "НЕ установлен. Скачайте: https://git-scm.com/downloads"
fi
echo ""

# 2. SSH-ключ или HTTPS-токен
echo "2. Авторизация на GitHub:"
echo "   Вариант A — SSH-ключ (рекомендуется):"
if [ -f "$HOME/.ssh/id_ed25519.pub" ] || [ -f "$HOME/.ssh/id_rsa.pub" ]; then
    PUB_KEY=$(ls $HOME/.ssh/id_*.pub 2>/dev/null | head -1)
    ok "SSH-ключ найден: ${PUB_KEY}"
    echo "      Проверьте, что он добавлен на GitHub:"
    echo "      https://github.com/settings/keys"
else
    warn "SSH-ключ не найден в $HOME/.ssh/"
    echo "      Создайте (выполните в терминале):"
    echo "        ssh-keygen -t ed25519 -C 'chervak_2016@mail.ru'"
    echo "        cat ~/.ssh/id_ed25519.pub"
    echo "      Скопируйте вывод и добавьте на GitHub:"
    echo "        https://github.com/settings/ssh/new"
fi
echo ""
echo "   Вариант B — HTTPS + Personal Access Token:"
echo "      Создайте токен: https://github.com/settings/tokens"
echo "      Generate new token (classic) → ☑ repo"
echo "      Скопируйте токен (вида ghp_xxxxxxxxxxxx)."
echo "      При первом git push введите username + токен как пароль."
echo ""

# 3. Глобальные настройки git
echo "3. Настройки git user.name и user.email:"
if git config --global user.name | grep -q .; then
    ok "user.name: $(git config --global user.name)"
else
    warn "user.name не задан. Установите:"
    echo "      git config --global user.name 'Червак Алексей Петрович'"
fi
if git config --global user.email | grep -q .; then
    ok "user.email: $(git config --global user.email)"
else
    warn "user.email не задан. Установите:"
    echo "      git config --global user.email 'chervak_2016@mail.ru'"
fi
echo ""

# 4. GitHub CLI (опционально)
echo "4. GitHub CLI (опционально, для удобства):"
if command -v gh >/dev/null 2>&1; then
    ok "установлен: $(gh --version | head -1)"
else
    warn "не установлен. Если поставите — упрощает создание репозитория:"
    echo "      Установка: https://cli.github.com/"
    echo "      После установки:"
    echo "        gh auth login"
    echo "        gh repo create data-platform --public --source=. --push"
fi
echo ""

# 5. Источник портфолио
echo "5. Папка портфолио:"
SOURCE_DIR="/home/z/my-project/download/portfolio/data-platform"
if [ -d "${SOURCE_DIR}" ]; then
    ok "найдена: ${SOURCE_DIR}"
    ok "файлов: $(find ${SOURCE_DIR} -type f | wc -l)"
    ok "размер: $(du -sh ${SOURCE_DIR} | cut -f1)"
else
    fail "НЕ найдена: ${SOURCE_DIR}"
fi
echo ""

# 6. Python (для GitHub Pages dbt docs, опционально)
echo "6. Python (опционально, для dbt docs на GitHub Pages):"
if command -v python3 >/dev/null 2>&1; then
    ok "установлен: $(python3 --version)"
else
    warn "не установлен (опционально)"
fi
echo ""

echo "════════════════════════════════════════════════════════════════"
echo "  Если все [OK] — можно запускать:"
echo ""
echo "    cd /home/z/my-project/download/portfolio"
echo "    bash publish.sh"
echo ""
echo "  Если есть [FAIL] — исправьте и запустите check_env.sh снова."
echo "════════════════════════════════════════════════════════════════"
