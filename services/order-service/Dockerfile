FROM node:18-alpine

WORKDIR /app

COPY services/order-service/package*.json ./
RUN npm install

COPY services/order-service/. ./
COPY shared ./shared

EXPOSE 3001

CMD ["npm", "start"]