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
{{- $allowExternalRedisURL := default false $redisValues.allowExternalUrlEnv -}}
- name: QUEUE_BACKEND
  value: {{ $queueBackend | quote }}
{{- if eq $queueBackend "redis" }}
{{- if not (or $redisSecretName .Values.config.redisUrl $allowExternalRedisURL) }}
{{- fail "REDIS_URL is required when config.queueBackend=redis; set redis.existingSecret.name, config.redisUrl, or redis.allowExternalUrlEnv=true when REDIS_URL is supplied through extraEnv/extraEnvFrom" }}
{{- end }}
{{- if and .Values.config.redisUrl (contains "@" .Values.config.redisUrl) }}
{{- fail "config.redisUrl appears to contain credentials; store credentialed Redis URLs in a Kubernetes Secret with redis.existingSecret.name instead" }}
{{- end }}
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

{{- define "open-sspm.oktaPushIngestEnabled" -}}
{{- $value := .Values.config.oktaPushIngestEnabled -}}
{{- if or (eq (toString $value) "") (eq (toString $value) "<nil>") -}}
{{- ternary "1" "0" .Values.config.syncDiscoveryEnabled -}}
{{- else -}}
{{- $normalized := lower (toString $value) -}}
{{- if or (eq $normalized "true") (eq $normalized "1") -}}1{{- else -}}0{{- end -}}
{{- end -}}
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

{{- define "open-sspm.oktaPushIngestEnv" -}}
- name: OKTA_PUSH_INGEST_ENABLED
  value: {{ include "open-sspm.oktaPushIngestEnabled" . | quote }}
- name: OKTA_PUSH_INGEST_BATCH_SIZE
  value: {{ .Values.config.oktaPushIngest.batchSize | quote }}
- name: OKTA_PUSH_INGEST_POLL_INTERVAL
  value: {{ .Values.config.oktaPushIngest.pollInterval | quote }}
- name: OKTA_PUSH_INGEST_CLEANUP_INTERVAL
  value: {{ .Values.config.oktaPushIngest.cleanupInterval | quote }}
- name: OKTA_PUSH_INGEST_RETRY_DELAY
  value: {{ .Values.config.oktaPushIngest.retryDelay | quote }}
- name: OKTA_PUSH_INGEST_RETRY_MAX_DELAY
  value: {{ .Values.config.oktaPushIngest.retryMaxDelay | quote }}
- name: OKTA_PUSH_INGEST_STALE_PROCESSING_TIMEOUT
  value: {{ .Values.config.oktaPushIngest.staleProcessingTimeout | quote }}
- name: OKTA_PUSH_INGEST_MAX_ATTEMPTS
  value: {{ .Values.config.oktaPushIngest.maxAttempts | quote }}
- name: OKTA_PUSH_INGEST_PROCESSED_RETENTION_DAYS
  value: {{ .Values.config.oktaPushIngest.processedRetentionDays | quote }}
- name: OKTA_PUSH_INGEST_DEAD_LETTER_RETENTION_DAYS
  value: {{ .Values.config.oktaPushIngest.deadLetterRetentionDays | quote }}
{{- end -}}

{{- define "open-sspm.tailSyncEnv" -}}
- name: SYNC_TAIL_INTERVAL
  value: {{ .Values.config.syncTailInterval | quote }}
{{- end -}}

{{- define "open-sspm.riskpolicyEventWorkerEnv" -}}
- name: RISKPOLICY_EVENT_WORKER_POLL_INTERVAL
  value: {{ .Values.config.riskpolicyEventWorker.pollInterval | quote }}
- name: RISKPOLICY_EVENT_WORKER_BATCH_SIZE
  value: {{ .Values.config.riskpolicyEventWorker.batchSize | quote }}
- name: RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS
  value: {{ .Values.config.riskpolicyEventWorker.maxAttempts | quote }}
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
