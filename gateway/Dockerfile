# The services side's gateway: nginx with one route table (nginx.conf).
#
#   docker build -t ghcr.io/ffetih/ff-logistic-services/gateway:local gateway
#
# nginx-unprivileged listens on a high port and writes only to /tmp (pid, temp files), so it runs
# under the chart's read-only root filesystem and non-root user.
FROM nginxinc/nginx-unprivileged:1.30-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
USER 10001
EXPOSE 8081
