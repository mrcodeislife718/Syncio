FROM node:22-alpine

ENV NODE_ENV=production \
    SYNCIO_DATA_FILE=/var/lib/syncio/data.syncio.json \
    SYNCIO_HOST=0.0.0.0 \
    SYNCIO_PORT=8787

WORKDIR /app
COPY package.json ./
COPY src ./src
COPY bin ./bin
RUN mkdir -p /var/lib/syncio && chown -R node:node /var/lib/syncio /app

USER node
VOLUME ["/var/lib/syncio"]
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD node -e "fetch('http://127.0.0.1:8787/health').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
CMD ["node","bin/syncio-server.js","serve"]
