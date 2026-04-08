FROM node:18-alpine

WORKDIR /app

COPY services/payment-service/package*.json ./
RUN npm install

COPY services/payment-service/. ./
COPY shared ./shared

EXPOSE 3002

CMD ["npm", "start"]