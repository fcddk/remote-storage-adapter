FROM registry-jinan-lab.inspurcloud.cn/library/common/prometheus/busybox-linux-amd64:latest
MAINTAINER Shuai Wu  <wushuai.lc@inspur.com>
ENV INFLUXDB_URL=http://influxdb:8086/
ENV INFLUXDB_DATABASE=prometheus
ENV INFLUXDB_RETENTION_POLICY=autogen
ENV INFLUXDB_USER=influx
ENV INFLUXDB_PW=influx
COPY remote-storage-adapter /usr/bin/remote-storage-adapter

EXPOSE 9201
CMD ["remote-storage-adapter"]
#CMD ["remote-storage-adapter", "-influxdb-url=$INFLUXDB_URL", "-influxdb.database=$INFLUXDB_DATABASE", "-influxdb.retention-policy=$INFLUXDB_RETENTION_POLICY", "-influxdb.username=$INFLUXDB_USER"]
