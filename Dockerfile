# Alternate: registry1.dso.mil/ironbank/opensource/python:v3.11.9
ARG BASE_IMAGE=registry.access.redhat.com/ubi9/python-311:1-52
FROM $BASE_IMAGE as build
USER root

RUN dnf install -y --nodocs java-11-openjdk-headless && \
	dnf clean all && \
	rm -rf /var/cache/dnf
ADD . /tmp
WORKDIR /tmp
RUN /tmp/add-dod-certs.sh
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon installDist

FROM $BASE_IMAGE
USER root
RUN dnf install -y --nodocs java-11-openjdk-headless dejavu-serif-fonts dejavu-sans-mono-fonts && \
	dnf clean all && \
	rm -rf /var/cache/dnf
RUN pip install cwms-python jpype1 awscli

COPY --from=build /etc/pki/ca-trust/source/anchors/* /etc/pki/ca-trust/source/anchors/
RUN /usr/bin/update-ca-trust

COPY --from=build /tmp/build/install /opt/
ADD --chmod=775 example.py /input/
ADD --chmod=775 entrypoint.sh /
ADD input /input/

WORKDIR /opt/pdf-gen
# The unzip is only if/when the PR is merged by HEC
# HEC modules originally for jython
#RUN unzip -j lib/jython-db-api-*.jar "*.py" -d hec_jython
ADD hec_jython hec_jython/

ADD --chmod=775 runjava.sh jython jpype jpype-wrapper.py ./
RUN chmod a+rx runjava.sh jython jpype jpype-wrapper.py ; chmod -R a+rX /input hec_jython lib

ENV UID=1000
ENV GID=1000
ENV OFFICE_ID=
ENV TZ=UTC
ENV CDA_URL=https://cwms-data.usace.army.mil:443/cwms-data
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV JAVA_XMS=256m
ENV JAVA_XMX=256m

RUN mkdir /data /output && chown $UID:$GID /input /data /output && chmod g+wt /input /data /output

# Input script location
VOLUME /input
# Extra data location (e.g. header/footer images)
VOLUME /data
# Output PDF location
VOLUME /output

USER $UID:$GID

WORKDIR /input

LABEL org.opencontainers.image.authors="Daniel.T.Osborne@usace.army.mil"

ENTRYPOINT [ "/entrypoint.sh" ]
