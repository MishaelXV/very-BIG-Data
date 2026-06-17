FROM flink:1.18-java11

# ── System packages ────────────────────────────────────────────────────────────
# openjdk-11-jdk-headless adds JDK headers (include/jni.h) missing from the
# base image, which ships only the JRE.  pemja (a C extension bundled inside
# apache-flink) requires these headers at compile time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless \
        python3 \
        python3-pip \
        python3-dev \
        build-essential \
        wget \
        curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# ── Python packages (PyFlink + PostgreSQL driver) ──────────────────────────────
# JAVA_HOME must point to the JDK (has include/) during pip install.
# The flink base image sets JAVA_HOME to the JRE (/opt/java/openjdk),
# which has no include/ directory.  We override it for this RUN step only:
#   1. Locate the freshly installed javac and walk up two directories to the
#      JDK root — works on both amd64 and arm64 without hardcoding the path.
#   2. Verify include/jni.h exists before starting the build.
#   3. Run pip install with the correct JAVA_HOME in scope.
COPY requirements.txt /tmp/flink-requirements.txt
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac)))) \
    && echo "[build] JAVA_HOME=${JAVA_HOME}" \
    && ls -la ${JAVA_HOME}/include \
    && pip3 install --no-cache-dir -r /tmp/flink-requirements.txt

# ── Flink Kafka connector JARs ─────────────────────────────────────────────────
# flink-connector-kafka: PyFlink Kafka source/sink (matches Flink 1.18)
# kafka-clients: Kafka protocol implementation required at runtime
RUN wget -q -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar" \
 && wget -q -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar"
