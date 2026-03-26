{{/* vim: set filetype=mustache: */}}
{{/*
Renders a value that contains template.
Usage:
{{ include "aistor.render" ( dict "value" .Values.path.to.the.Value "context" $) }}
*/}}
{{- define "aistor.render" -}}
  {{- if typeIs "string" .value }}
    {{- tpl .value .context }}
  {{- else }}
    {{- tpl (.value | toYaml) .context }}
  {{- end }}
{{- end -}}

{{/*
    aistor.isOpenShift returns whether the current cluster is OpenShift.
*/}}
{{- define "aistor.isOpenShift" -}}
{{- if ($.Values.forceOpenShift | default (.Capabilities.APIVersions.Has "security.openshift.io/v1/SecurityContextConstraints")) -}}
{{- true -}}
{{- end -}}
{{- end -}}
