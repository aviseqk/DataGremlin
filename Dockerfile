# Dockerfile for Redis Server

FROM redis:alpine

EXPOSE 6379

VOLUME /data

CMD ["redis-server"]
