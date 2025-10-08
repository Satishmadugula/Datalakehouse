{{- define "nessie.fullname" -}}
{{- printf "%s-nessie" .Release.Name -}}
{{- end -}}
