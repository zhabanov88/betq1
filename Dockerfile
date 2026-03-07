FROM node:20-alpine

# Нужно для компиляции bcrypt (нативный модуль)
RUN apk add --no-cache python3 make g++

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server/ ./server/
COPY public/  ./public/

EXPOSE 3000
CMD ["node", "server/index.js"]