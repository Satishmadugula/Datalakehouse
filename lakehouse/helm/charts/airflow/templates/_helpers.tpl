{{- define "airflow.web.fullname" -}}
{{- printf "%s-airflow-web" .Release.Name -}}
{{- end -}}
{{- define "airflow.scheduler.fullname" -}}
{{- printf "%s-airflow-scheduler" .Release.Name -}}
{{- end -}}
{{- define "airflow.serviceAccount" -}}
{{- printf "%s-airflow" .Release.Name -}}
{{- end -}}
