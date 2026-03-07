FROM node:20-alpine

# Нужно для компиляции bcrypt (нативный модуль) + python3 для ETL
RUN apk add --no-cache python3 py3-pip make g++ curl

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server/  ./server/
COPY public/  ./public/

# Копируем ETL скрипты (запускаются через /api/etl/run)
COPY betquant-etl/   ./betquant-etl/
COPY sports-etl-v2/  ./sports-etl-v2/

# Устанавливаем Python-зависимости для ETL v1
RUN pip3 install requests beautifulsoup4 lxml --break-system-packages 2>/dev/null || true

# Устанавливаем Python-зависимости для ETL v2 (если есть requirements файл)
RUN if [ -f sports-etl-v2/requirements_etl_v2.txt ]; then \
      pip3 install -r sports-etl-v2/requirements_etl_v2.txt --break-system-packages 2>/dev/null || true; \
    fi

EXPOSE 3000
CMD ["node", "server/index.js"]