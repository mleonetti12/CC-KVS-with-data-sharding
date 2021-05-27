 FROM node:12-alpine
 RUN apk add --no-cache python g++ make
 WORKDIR /CSE138_Assignment
 COPY . .
 RUN yarn install --production
 CMD ["node", "app/src/index.js"]
