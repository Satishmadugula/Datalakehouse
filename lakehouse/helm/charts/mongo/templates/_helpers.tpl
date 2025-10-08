{{- define "mongo.fullname" -}}
{{- printf "%s-mongo" .Release.Name -}}
{{- end -}}
