{{- define "hsmfs.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hsmfs.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hsmfs.labels" -}}
app.kubernetes.io/name: {{ include "hsmfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hsm
{{- end -}}

{{- define "hsmfs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hsmfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hsmfs.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "hsmfs.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Image reference: digest wins over tag, so a release can be pinned to exactly the signed artifact. */}}
{{- define "hsmfs.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{/* Fail fast on settings the service would reject at startup anyway -- better at `helm install` than as a CrashLoopBackOff. */}}
{{- define "hsmfs.validate" -}}
{{- if not .Values.config.access.allowedPathPrefixes -}}
{{- fail "config.access.allowedPathPrefixes must list at least one prefix (use [\"*\"] to allow every path explicitly)" -}}
{{- end -}}
{{- if not .Values.config.core.baseUrl -}}
{{- fail "config.core.baseUrl is required (hsm-core-service URL)" -}}
{{- end -}}
{{- if not .Values.config.core.appId -}}
{{- fail "config.core.appId is required (this service's own app_id in hsm-core-service)" -}}
{{- end -}}
{{- if not .Values.config.store.root -}}
{{- fail "config.store.root is required" -}}
{{- end -}}
{{- if and .Values.istio.authorizationPolicy.enabled (not .Values.istio.authorizationPolicy.bffPrincipals) -}}
{{- fail "istio.authorizationPolicy.bffPrincipals must name the BFF's service account (e.g. cluster.local/ns/<ns>/sa/<bff-sa>)" -}}
{{- end -}}
{{- if and (eq .Values.config.core.authMode "AZURE_AD") (not .Values.config.core.azureTokenScope) -}}
{{- fail "config.core.azureTokenScope is required when config.core.authMode is AZURE_AD" -}}
{{- end -}}
{{- if and (not .Values.secrets.keyVault.enabled) (not .Values.secrets.existingSecretName) -}}
{{- fail "provide the app private key either via secrets.keyVault (recommended) or secrets.existingSecretName" -}}
{{- end -}}
{{- end -}}
