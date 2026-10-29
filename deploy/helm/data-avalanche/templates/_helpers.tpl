{{/* The release's name for its objects, at most 63 characters. */}}
{{- define "data-avalanche.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "data-avalanche.headless" -}}
{{- printf "%s-headless" (include "data-avalanche.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "data-avalanche.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "data-avalanche.labels" -}}
{{ include "data-avalanche.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "data-avalanche.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "data-avalanche.fullname" .) -}}
{{- else -}}
{{- .Values.serviceAccount.name | default "default" -}}
{{- end -}}
{{- end -}}

{{- define "data-avalanche.configMap" -}}
{{- .Values.existingConfigMap | default (include "data-avalanche.fullname" .) -}}
{{- end -}}

{{/* The config as a map: {} when it is given as text (ports and TLS then come from values). */}}
{{- define "data-avalanche.configMap.values" -}}
{{- if kindIs "map" .Values.config -}}{{- toYaml .Values.config -}}{{- else -}}{}{{- end -}}
{{- end -}}

{{/* The API's port: globals.api.address's, else ports.api. */}}
{{- define "data-avalanche.apiPort" -}}
{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
{{- $address := dig "globals" "api" "address" "" $config -}}
{{- if $address -}}
{{- last (splitList ":" $address) -}}
{{- else -}}
{{- .Values.ports.api -}}
{{- end -}}
{{- end -}}

{{/* The gossip's port: memberlist.bind_port's, else ports.gossip. */}}
{{- define "data-avalanche.gossipPort" -}}
{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
{{- dig "globals" "cluster" "memberlist" "bind_port" .Values.ports.gossip $config -}}
{{- end -}}

{{/* HTTPS when the config turns the API's TLS on. */}}
{{- define "data-avalanche.scheme" -}}
{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
{{- if dig "globals" "api" "tls" "enabled" (.Values.apiTls | default false) $config -}}HTTPS{{- else -}}HTTP{{- end -}}
{{- end -}}

{{/* A probe from values, its scheme the API's. */}}
{{- define "data-avalanche.probe" -}}
{{- $probe := deepCopy .probe -}}
{{- if hasKey $probe "httpGet" -}}
{{- $_ := set $probe.httpGet "scheme" (include "data-avalanche.scheme" .root) -}}
{{- end -}}
{{- toYaml $probe -}}
{{- end -}}
