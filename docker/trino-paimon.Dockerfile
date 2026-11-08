# # docker/paimon-trino.Dockerfile
# # -----------------------------------------------------------------------------
# # Stage 1: Builder
# # -----------------------------------------------------------------------------
# FROM maven:3.9-eclipse-temurin-21 AS builder

# WORKDIR /build

# # --- Step A: Install Paimon Parent ---
# RUN git clone --depth 1 https://github.com/apache/paimon.git paimon-main && \
#     cd paimon-main && \
#     mvn clean install -N -DskipTests

# # --- Step B: Build Plugin (WITH NATIVE AZURE PATCH) ---
# RUN cd paimon-main && \
#     PAIMON_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \
#     cd /build && \
#     git clone --depth 1 https://github.com/apache/paimon-trino.git paimon-trino && \
#     cd paimon-trino && \
#     # Sync Version
#     sed -i "s/<version>1.0-SNAPSHOT<\/version>/<version>${PAIMON_VERSION}<\/version>/g" pom.xml && \
#     # -------------------------------------------------------------------------
#     # CRITICAL FIX: Disable HDFS Module Loading
#     # We remove the lines that activate HDFS. This allows us to run without Hadoop jars.
#     # -------------------------------------------------------------------------
#     FACTORY_FILE=$(find . -name "TrinoConnectorFactory.java") && \
#     echo "Patching file: $FACTORY_FILE" && \
#     sed -i '/HdfsModule/d' "$FACTORY_FILE" && \
#     sed -i '/HdfsAuthenticationModule/d' "$FACTORY_FILE" && \
#     # VERIFY Patch
#     if grep -q "new HdfsModule()" "$FACTORY_FILE"; then echo "❌ Patch Failed"; exit 1; else echo "✅ Patch Applied"; fi && \
#     # Build
#     mvn clean package -DskipTests -Dspotless.check.skip=true -Dtrino.version=440

# # --- Step C: Extract and Filter Jars (THE HYBRID FIX) ---
# RUN mkdir -p /build/plugin-extracted && \
#     PLUGIN_TAR=$(find paimon-trino -name "paimon-trino-*-plugin.tar.gz" | head -n 1) && \
#     tar -xf "${PLUGIN_TAR}" -C /build/plugin-extracted --strip-components=1 && \
#     # -------------------------------------------------------------------------
#     # ACTION: 
#     # 1. DELETE 'hadoop-apache' (Fixes Trino Security Error)
#     # 2. KEEP 'trino-hdfs' (Fixes Paimon ClassNotFound Error)
#     # -------------------------------------------------------------------------
#     rm -f /build/plugin-extracted/hadoop-apache-*.jar && \
#     echo "✅ Removed hadoop-apache jar (Security compliance)." && \
#     # Verify trino-hdfs is STILL there
#     ls -l /build/plugin-extracted/trino-hdfs-*.jar && \
#     echo "✅ Verified trino-hdfs jar is present (Dependency compliance)."

# # -----------------------------------------------------------------------------
# # Stage 2: Final Image
# # -----------------------------------------------------------------------------
# FROM trinodb/trino:440

# USER root

# # Copy the plugin
# COPY --from=builder --chown=trino:trino /build/plugin-extracted /usr/lib/trino/plugin/paimon

# # Ensure catalog directory
# RUN mkdir -p /etc/trino/catalog && \
#     chown -R trino:trino /etc/trino/catalog

# USER trino
# # End of docker/trino-paimon.Dockerfile


###########################################################################################################################
# docker/trino-paimon.Dockerfile
#
# Implements the workaround described in the GitHub issue comment:
# - Paimon Trino connector from Apache snapshots (paimon-trino-440 1.0-SNAPSHOT, classifier=plugin, ext=tar.gz)
# - Add Guava 31.0-jre + failureaccess 1.0.1 into plugin/paimon
# - Rebuild trino-filesystem-manager-440.jar and comment out:
#     verify(clazz == null, "HDFS should not be on the plugin classpath");
# - Copy the patched trino-filesystem-manager jar into plugin/paimon
# - Create plugin/paimon/hdfs and copy all jars into it
# - Remove tpch/tpcds plugins (as the commenter did)

ARG TRINO_VERSION=440

# -----------------------------------------------------------------------------
# Stage 1: Builder (downloads plugin + rebuilds patched filesystem manager)
# -----------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS builder

ARG TRINO_VERSION
ARG GUAVA_VERSION=31.0-jre
ARG FAILUREACCESS_VERSION=1.0.1
ARG APACHE_SNAPSHOTS_REPO=https://repository.apache.org/content/groups/snapshots
ARG PAIMON_TRINO_SNAPSHOT_VERSION=1.0-SNAPSHOT

# Use bash for RUN lines (avoids dash /bin/sh quirks; see Dockerfile SHELL docs)
SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ---- Download paimon-trino plugin tar.gz from Apache snapshots (maven-metadata.xml)
RUN BASE_URL="${APACHE_SNAPSHOTS_REPO}/org/apache/paimon/paimon-trino-${TRINO_VERSION}/${PAIMON_TRINO_SNAPSHOT_VERSION}" \
 && METADATA_URL="${BASE_URL}/maven-metadata.xml" \
 && echo "Fetching ${METADATA_URL}" \
 && curl -fsSL "${METADATA_URL}" -o /tmp/maven-metadata.xml \
 && SNAPSHOT_VALUE="$(python3 -c 'import xml.etree.ElementTree as ET; r=ET.parse("/tmp/maven-metadata.xml").getroot(); v=[(sv.findtext("value") or "").strip() for sv in r.findall(".//snapshotVersion") if (sv.findtext("extension") or "").strip()=="tar.gz" and (sv.findtext("classifier") or "").strip()=="plugin" and (sv.findtext("value") or "").strip()]; print(v[-1] if v else "")')" \
 && [ -n "${SNAPSHOT_VALUE}" ] \
 && FILE="paimon-trino-${TRINO_VERSION}-${SNAPSHOT_VALUE}-plugin.tar.gz" \
 && echo "Downloading ${BASE_URL}/${FILE}" \
 && curl -fsSL "${BASE_URL}/${FILE}" -o /tmp/paimon-trino-plugin.tar.gz

# ---- Extract plugin tarball into /build/plugin/paimon
RUN set -eux; \
    rm -rf /build/plugin; \
    mkdir -p /build/plugin; \
    tar -xzf /tmp/paimon-trino-plugin.tar.gz -C /build/plugin; \
    # Normalize to /build/plugin/paimon (tar often has a top-level directory) \
    if [ -d /build/plugin/paimon ]; then \
      echo "Found /build/plugin/paimon"; \
    else \
      top="$(find /build/plugin -maxdepth 1 -type d ! -path /build/plugin -print -quit)"; \
      test -n "${top}"; \
      mv "${top}" /build/plugin/paimon; \
    fi; \
    ls -lah /build/plugin/paimon

# ---- Download guava + failureaccess into plugin/paimon (workaround)
RUN curl -fsSL -o "/build/plugin/paimon/guava-${GUAVA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
 && curl -fsSL -o "/build/plugin/paimon/failureaccess-${FAILUREACCESS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/google/guava/failureaccess/${FAILUREACCESS_VERSION}/failureaccess-${FAILUREACCESS_VERSION}.jar" \
 && ls -lah /build/plugin/paimon | sed -n '1,120p'

# ---- Rebuild trino-filesystem-manager-${TRINO_VERSION}.jar with verify() commented out
RUN rm -rf /tmp/trino \
 && git clone https://github.com/trinodb/trino.git /tmp/trino \
 && cd /tmp/trino \
 && git fetch --tags \
 && git checkout "tags/${TRINO_VERSION}" \
 && cd lib/trino-filesystem-manager \
 && FILE="src/main/java/io/trino/filesystem/manager/HdfsFileSystemLoader.java" \
 && grep -n 'HDFS should not be on the plugin classpath' "${FILE}" \
 && sed -i 's/^[[:space:]]*verify(clazz == null, "HDFS should not be on the plugin classpath");/        \/\/ verify(clazz == null, "HDFS should not be on the plugin classpath");/' "${FILE}" \
 && grep -n 'HDFS should not be on the plugin classpath' "${FILE}" \
 && mvn -B -DskipTests package \
 && cp -v "target/trino-filesystem-manager-${TRINO_VERSION}.jar" /build/plugin/paimon/

# ---- Create hdfs/ and copy all jars into it (exactly like the issue comment)
RUN mkdir -p /build/plugin/paimon/hdfs \
 && cp -v /build/plugin/paimon/*.jar /build/plugin/paimon/hdfs/ \
 && ls -lah /build/plugin/paimon/hdfs | sed -n '1,120p'

# -----------------------------------------------------------------------------
# Stage 2: Final Trino image
# -----------------------------------------------------------------------------
FROM trinodb/trino:${TRINO_VERSION}

USER root

# Install paimon plugin directory into Trino plugin directory
COPY --from=builder --chown=trino:trino /build/plugin/paimon /usr/lib/trino/plugin/paimon

# Remove tpch/tpcds plugins (as the issue comment did)
RUN rm -rf /usr/lib/trino/plugin/tpcds /usr/lib/trino/plugin/tpch || true \
 && ls -lah /usr/lib/trino/plugin/paimon | sed -n '1,120p'

USER trino
