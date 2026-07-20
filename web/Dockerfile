FROM node:22-alpine AS build
WORKDIR /app
COPY web/package*.json ./
RUN npm install
COPY web .
RUN npm run build

FROM nginx:1.27-alpine
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
RUN chown -R nginx:nginx /var/cache/nginx /var/run /usr/share/nginx/html && sed -i 's/user  nginx;/user nginx;/' /etc/nginx/nginx.conf
USER nginx
EXPOSE 3000
