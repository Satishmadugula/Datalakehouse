{{- define "dremio.fullname" -}}
{{- printf "%s-dremio" .Release.Name -}}
{{- end -}}
