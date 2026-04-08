FROM node:18-alpine

WORKDIR /app

COPY services/inventory-service/package*.json ./
RUN npm install

COPY services/inventory-service/. ./
COPY shared ./shared

EXPOSE 3003

CMD ["npm", "start"]