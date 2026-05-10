{{- define "open-sspm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "open-sspm.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "open-sspm.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "open-sspm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "open-sspm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "open-sspm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "open-sspm.labels" -}}
helm.sh/chart: {{ include "open-sspm.chart" . }}
{{ include "open-sspm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "open-sspm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "open-sspm.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "open-sspm.smtpEnv" -}}
- name: SMTP_ENABLED
  value: {{ ternary "1" "0" .Values.smtp.enabled | quote }}
{{- if .Values.smtp.enabled }}
- name: SMTP_HOST
  value: {{ .Values.smtp.host | quote }}
- name: SMTP_PORT
  value: {{ .Values.smtp.port | quote }}
- name: SMTP_TLS_MODE
  value: {{ .Values.smtp.tlsMode | quote }}
- name: SMTP_FROM_ADDRESS
  value: {{ .Values.smtp.fromAddress | quote }}
- name: SMTP_FROM_NAME
  value: {{ .Values.smtp.fromName | quote }}
{{- if .Values.smtp.existingSecret.name }}
- name: SMTP_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.smtp.existingSecret.name }}
      key: {{ required "smtp.existingSecret.usernameKey is required when smtp.existingSecret.name is set" .Values.smtp.existingSecret.usernameKey | quote }}
      optional: false
- name: SMTP_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.smtp.existingSecret.name }}
      key: {{ required "smtp.existingSecret.passwordKey is required when smtp.existingSecret.name is set" .Values.smtp.existingSecret.passwordKey | quote }}
      optional: false
{{- end }}
{{- end }}
{{- end -}}

{{- define "open-sspm.queueEnv" -}}
{{- $queueBackend := default "postgres" .Values.config.queueBackend | lower -}}
{{- $redisValues := default dict .Values.redis -}}
{{- $redisSecret := default dict $redisValues.existingSecret -}}
{{- $redisSecretName := default "" $redisSecret.name -}}
{{- $redisSecretURLKey := default "REDIS_URL" $redisSecret.urlKey -}}
- name: QUEUE_BACKEND
  value: {{ $queueBackend | quote }}
{{- if eq $queueBackend "redis" }}
{{- if $redisSecretName }}
- name: REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ $redisSecretName | quote }}
      key: {{ $redisSecretURLKey | quote }}
      optional: false
{{- else if .Values.config.redisUrl }}
- name: REDIS_URL
  value: {{ .Values.config.redisUrl | quote }}
{{- end }}
- name: REDIS_KEY_PREFIX
  value: {{ default "open-sspm" .Values.config.redisKeyPrefix | quote }}
{{- end }}
{{- end -}}
