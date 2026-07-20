{{- define "runmesh.name" -}}runmesh{{- end }}
{{- define "runmesh.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "runmesh.name" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}
{{- define "runmesh.secretName" -}}
{{- default (include "runmesh.name" .) .Values.secret.existingSecret -}}
{{- end }}
{{- define "runmesh.labels" -}}
app.kubernetes.io/name: {{ include "runmesh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: runmesh
{{- end }}
