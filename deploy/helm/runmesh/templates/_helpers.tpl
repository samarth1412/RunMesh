{{- define "runmesh.name" -}}runmesh{{- end }}
{{- define "runmesh.labels" -}}
app.kubernetes.io/name: {{ include "runmesh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
