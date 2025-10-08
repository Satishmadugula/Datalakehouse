{{- define "spark.fullname" -}}
{{- printf "%s-spark" .Release.Name -}}
{{- end -}}
