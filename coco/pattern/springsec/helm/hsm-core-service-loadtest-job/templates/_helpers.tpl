{{- define "hsm.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hsm.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hsm.labels" -}}
app.kubernetes.io/name: {{ include "hsm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "hsm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hsm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
