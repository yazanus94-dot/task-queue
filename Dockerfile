FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

COPY src/ ./src/
COPY tests/ ./tests/

USER node
EXPOSE 3000
ENTRYPOINT ["node", "src/index.js"]