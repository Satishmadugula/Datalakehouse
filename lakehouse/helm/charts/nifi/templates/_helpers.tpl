{{- define "nifi.fullname" -}}
{{- printf "%s-nifi" .Release.Name -}}
{{- end -}}
