# Stage 1: build the React app
FROM node:20-alpine AS build
WORKDIR /app

# Accept backend URL from docker-compose
ARG REACT_APP_API_URL
ENV REACT_APP_API_URL=$REACT_APP_API_URL

COPY package*.json ./
# More deterministic than npm install and faster in CI
RUN npm ci

# Bring in sources
COPY . .

# Make sure shims are executable (belt-and-suspenders)
RUN chmod -R a+x node_modules/.bin || true

# Call react-scripts directly via node to avoid .bin shim quirks
RUN node node_modules/react-scripts/scripts/build.js

# Stage 2: serve with nginx
FROM nginx:alpine

# Add custom nginx config for SPA routing
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy build output
COPY --from=build /app/build /usr/share/nginx/html

EXPOSE 80
