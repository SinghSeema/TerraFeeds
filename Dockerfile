FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build && npm prune --omit=dev
EXPOSE 3000
CMD ["node", "dist/server.js"]
