ARG GRAFANA_IMAGE
FROM ${GRAFANA_IMAGE}

ARG ADX_PLUGIN_ID=grafana-azure-data-explorer-datasource
ARG ADX_PLUGIN_VERSION=7.2.8

USER root
ENV GF_PATHS_PLUGINS=/opt/grafana/plugins
RUN mkdir -p "${GF_PATHS_PLUGINS}" \
 && grafana cli --pluginsDir "${GF_PATHS_PLUGINS}" plugins install "${ADX_PLUGIN_ID}" "${ADX_PLUGIN_VERSION}" \
 && test -f "${GF_PATHS_PLUGINS}/${ADX_PLUGIN_ID}/plugin.json" \
 && chown -R 472:0 /opt/grafana

COPY provisioning /etc/grafana/provisioning
RUN chown -R 472:0 /etc/grafana && chmod -R g+rX /etc/grafana
USER 472
