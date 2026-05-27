{{/*
Chart name (truncated to 63 chars as required by Kubernetes).
*/}}
{{- define "pattern-assessment.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Chart label: name-version, used in helm.sh/chart label.
*/}}
{{- define "pattern-assessment.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "pattern-assessment.labels" -}}
helm.sh/chart: {{ include "pattern-assessment.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{/* ── Backend ──────────────────────────────────────────────────────────────── */}}

{{- define "pattern-assessment.backend.name" -}}
{{- printf "%s-backend" (include "pattern-assessment.name" .) }}
{{- end }}

{{- define "pattern-assessment.backend.labels" -}}
{{ include "pattern-assessment.labels" . }}
app.kubernetes.io/name: {{ include "pattern-assessment.backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}-backend
app.kubernetes.io/component: backend
{{- end }}

{{- define "pattern-assessment.backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pattern-assessment.backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}-backend
{{- end }}

{{/* ── Frontend ─────────────────────────────────────────────────────────────── */}}

{{- define "pattern-assessment.frontend.name" -}}
{{- printf "%s-frontend" (include "pattern-assessment.name" .) }}
{{- end }}

{{- define "pattern-assessment.frontend.labels" -}}
{{ include "pattern-assessment.labels" . }}
app.kubernetes.io/name: {{ include "pattern-assessment.frontend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}-frontend
app.kubernetes.io/component: frontend
{{- end }}

{{- define "pattern-assessment.frontend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pattern-assessment.frontend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}-frontend
{{- end }}

{{/* ── Service Account ──────────────────────────────────────────────────────── */}}

{{- define "pattern-assessment.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- printf "%s-sa" (include "pattern-assessment.name" .) }}
{{- else }}
{{- default "default" }}
{{- end }}
{{- end }}

{{/* ── Full image reference ─────────────────────────────────────────────────── */}}

{{- define "pattern-assessment.backend.image" -}}
{{- printf "%s/%s:%s" .Values.global.registry .Values.backend.image .Values.backend.tag }}
{{- end }}

{{- define "pattern-assessment.frontend.image" -}}
{{- printf "%s/%s:%s" .Values.global.registry .Values.frontend.image .Values.frontend.tag }}
{{- end }}
