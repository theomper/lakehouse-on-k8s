# docker/flink-paimon-kafka.Dockerfile
# ─────────────────────────────────────────────────────────────────────
#  Image: Flink 1.20.2 slim  +  all runtime JARs  +  Azure-FS plugin
# ─────────────────────────────────────────────────────────────────────
FROM apache/flink:1.20.2-scala_2.12

# 1️⃣ Copy all runtime and plugin jars into the lib directory
#  - flink-connector-kafka-1.20.2.jar (or matching minor)
#  - paimon-flink-1.20-*.jar
#  - paimon-azure-*.jar
#  - flink-shaded-hadoop-2-uber-*.jar
#  - flink-azure-fs-hadoop-*.jar
COPY lib/ /opt/flink/lib/

# 2️⃣ Move Azure FS plugin jar to its correct plugin directory (and remove from lib)
RUN set -e; \
    mkdir -p /opt/flink/plugins/flink-azure-fs-hadoop; \
    if [ -f /opt/flink/lib/flink-azure-fs-hadoop-1.20.2.jar ]; then \
      mv /opt/flink/lib/flink-azure-fs-hadoop-1.20.2.jar /opt/flink/plugins/flink-azure-fs-hadoop/; \
    fi

# 2b️⃣ Defense in depth: ensure no flink-shaded-hadoop-2-uber jar leaks into
#      /opt/flink/lib/ from a stale local docker/lib/ (CI no longer downloads
#      it, but a developer rebuilding locally may still have the file). Its
#      Hadoop 2.8.3 classes conflict with the Hadoop 3.x bundled in the Azure
#      FS plugin and break checkpoints to abfs://.
RUN rm -f /opt/flink/lib/flink-shaded-hadoop-2-uber-*.jar

# 3️⃣ Symlink any optional plugins from /opt/flink/opt/ to /opt/flink/plugins/
#    (Flink will auto-discover plugins placed this way)
RUN set -e; \
    for jar in /opt/flink/opt/*.jar; do \
      [ -e "$jar" ] || continue; \
      plugin_name="$(basename "$jar" .jar)"; \
      mkdir -p "/opt/flink/plugins/${plugin_name}"; \
      ln -sf "$jar" "/opt/flink/plugins/${plugin_name}/"; \
    done

# No changes to ENTRYPOINT or CMD
