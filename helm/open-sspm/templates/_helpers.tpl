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

{{- define "open-sspm.eventInboxEnabled" -}}
{{- $value := .Values.config.eventInboxEnabled -}}
{{- $normalized := lower (toString $value) -}}
{{- if or (eq $normalized "true") (eq $normalized "1") -}}1{{- else -}}0{{- end -}}
{{- end -}}

{{- define "open-sspm.fullSyncEnv" -}}
- name: SYNC_FULL_ENABLED
  value: {{ ternary "1" "0" .Values.config.syncFullEnabled | quote }}
- name: SYNC_INTERVAL
  value: {{ .Values.config.syncInterval | quote }}
{{- if .Values.config.syncOktaInterval }}
- name: SYNC_OKTA_INTERVAL
  value: {{ .Values.config.syncOktaInterval | quote }}
{{- end }}
{{- if .Values.config.syncEntraInterval }}
- name: SYNC_ENTRA_INTERVAL
  value: {{ .Values.config.syncEntraInterval | quote }}
{{- end }}
{{- if .Values.config.syncGoogleWorkspaceInterval }}
- name: SYNC_GOOGLE_WORKSPACE_INTERVAL
  value: {{ .Values.config.syncGoogleWorkspaceInterval | quote }}
{{- end }}
{{- if .Values.config.syncGithubInterval }}
- name: SYNC_GITHUB_INTERVAL
  value: {{ .Values.config.syncGithubInterval | quote }}
{{- end }}
{{- if .Values.config.syncDatadogInterval }}
- name: SYNC_DATADOG_INTERVAL
  value: {{ .Values.config.syncDatadogInterval | quote }}
{{- end }}
{{- if .Values.config.syncAwsInterval }}
- name: SYNC_AWS_INTERVAL
  value: {{ .Values.config.syncAwsInterval | quote }}
{{- end }}
{{- if .Values.config.syncFailureBackoffMax }}
- name: SYNC_FAILURE_BACKOFF_MAX
  value: {{ .Values.config.syncFailureBackoffMax | quote }}
{{- end }}
{{- end -}}

{{- define "open-sspm.eventInboxEnv" -}}
- name: EVENT_INBOX_ENABLED
  value: {{ include "open-sspm.eventInboxEnabled" . | quote }}
- name: EVENT_INBOX_BATCH_SIZE
  value: {{ .Values.config.eventInbox.batchSize | quote }}
- name: EVENT_INBOX_POLL_INTERVAL
  value: {{ .Values.config.eventInbox.pollInterval | quote }}
- name: EVENT_INBOX_CLEANUP_INTERVAL
  value: {{ .Values.config.eventInbox.cleanupInterval | quote }}
- name: EVENT_INBOX_RETRY_DELAY
  value: {{ .Values.config.eventInbox.retryDelay | quote }}
- name: EVENT_INBOX_RETRY_MAX_DELAY
  value: {{ .Values.config.eventInbox.retryMaxDelay | quote }}
- name: EVENT_INBOX_STALE_PROCESSING_TIMEOUT
  value: {{ .Values.config.eventInbox.staleProcessingTimeout | quote }}
- name: EVENT_INBOX_MAX_ATTEMPTS
  value: {{ .Values.config.eventInbox.maxAttempts | quote }}
- name: EVENT_INBOX_PROCESSED_RETENTION_DAYS
  value: {{ .Values.config.eventInbox.processedRetentionDays | quote }}
- name: EVENT_INBOX_DEAD_LETTER_RETENTION_DAYS
  value: {{ .Values.config.eventInbox.deadLetterRetentionDays | quote }}
{{- end -}}

{{- define "open-sspm.tailSyncEnv" -}}
- name: SYNC_TAIL_INTERVAL
  value: {{ .Values.config.syncTailInterval | quote }}
{{- end -}}

{{- define "open-sspm.eventEvaluatorWorkerEnv" -}}
- name: EVENT_EVALUATOR_WORKER_POLL_INTERVAL
  value: {{ .Values.config.eventEvaluatorWorker.pollInterval | quote }}
- name: EVENT_EVALUATOR_WORKER_BATCH_SIZE
  value: {{ .Values.config.eventEvaluatorWorker.batchSize | quote }}
- name: EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS
  value: {{ .Values.config.eventEvaluatorWorker.maxAttempts | quote }}
{{- end -}}

{{- define "open-sspm.eventPartitionEnv" -}}
- name: EVENT_PARTITION_MAINTENANCE_INTERVAL
  value: {{ .Values.config.eventPartitionMaintenanceInterval | quote }}
- name: EVENT_PARTITION_FUTURE_DAYS
  value: {{ .Values.config.eventPartitionFutureDays | quote }}
- name: EVENT_RETENTION_DAYS
  value: {{ .Values.config.eventRetentionDays | quote }}
{{- end -}}

{{- define "open-sspm.workerProbes" -}}
{{- $metricsAddr := lower (toString .Values.config.metricsAddr) -}}
{{- if and $metricsAddr (not (or (eq $metricsAddr "off") (eq $metricsAddr "disabled") (eq $metricsAddr "false"))) }}
livenessProbe:
  exec:
    command:
      - wget
      - --no-verbose
      - --tries=1
      - --spider
      - http://127.0.0.1:{{ .Values.metrics.port }}/healthz
  initialDelaySeconds: 20
  periodSeconds: 30
  timeoutSeconds: 3
  failureThreshold: 3
readinessProbe:
  exec:
    command:
      - wget
      - --no-verbose
      - --tries=1
      - --spider
      - http://127.0.0.1:{{ .Values.metrics.port }}/healthz
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
{{- end }}
{{- end -}}
