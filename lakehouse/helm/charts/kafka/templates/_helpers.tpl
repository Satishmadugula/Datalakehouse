{{- define "kafka.fullname" -}}
{{- printf "%s-kafka" .Release.Name -}}
{{- end -}}
{{- define "kafka.zookeeper" -}}
{{- printf "%s-zookeeper" .Release.Name -}}
{{- end -}}
