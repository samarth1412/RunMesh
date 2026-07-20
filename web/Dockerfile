FROM node:22-alpine AS build
WORKDIR /app
COPY web/package*.json ./
RUN npm ci
COPY web .
ARG WEB_DEVELOPMENT_MODE=true
ARG VITE_OIDC_AUTHORITY=http://localhost:8180/realms/runmesh
ARG VITE_OIDC_CLIENT_ID=runmesh-web
RUN VITE_DEV_AUTH="$WEB_DEVELOPMENT_MODE" \
    VITE_OIDC_AUTHORITY="$VITE_OIDC_AUTHORITY" \
    VITE_OIDC_CLIENT_ID="$VITE_OIDC_CLIENT_ID" \
    npm run build

FROM nginx:1.27-alpine
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
RUN chown -R nginx:nginx /var/cache/nginx /usr/share/nginx/html \
    && sed -i 's/user  nginx;/user nginx;/' /etc/nginx/nginx.conf \
    && sed -i 's|pid .*;|pid /tmp/nginx.pid;|' /etc/nginx/nginx.conf
USER 101:101
EXPOSE 3000
