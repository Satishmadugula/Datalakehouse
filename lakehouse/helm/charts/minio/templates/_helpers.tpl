{{- define "minio.fullname" -}}
{{- printf "%s-minio" .Release.Name -}}
{{- end -}}
