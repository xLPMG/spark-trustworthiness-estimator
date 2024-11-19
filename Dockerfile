FROM apache/spark:3.5.3
WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER root
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark

# Switch back to the spark user for application runtime
USER spark

COPY target/scala-2.12 /app/target/scala-2.12
COPY submitCluster.sh /app/submitCluster.sh
COPY spark-defaults.conf /opt/spark/conf/

CMD ["/app/submitCluster.sh"]