{{/* 
    aistor.namespace returns the namespace for AIStor resources.
*/}}
{{- define "aistor.namespace" -}}
{{ $.Values.namespaceOverride | default $.Release.Namespace| trunc 63 | trimSuffix "-" | quote }}
{{- end -}}

{{/* 
    aistor.resolveImage resolves the given name into a fully qualified image name.
    - name: the name of the image in $.images
    - root: the root object
    - val: the value of the image in the current object
*/}}
{{- define "aistor.resolveImage" -}}
{{- if .val.image -}}
  {{- .val.image -}}
{{- else -}}
  {{- $imageDef := index .root.images .name -}}
  {{- if not $imageDef -}}
    {{ fail (printf "Image '%s' not found in images definition" .name) }}
  {{- else if kindIs "string" $imageDef }}
    {{- $imageDef -}}
  {{- else if kindIs "map" $imageDef }}
    {{- if (not $imageDef.repository) }}
      {{ fail (printf "Image definition for '%s' must have a 'repository' field" .name) }}
    {{- end -}}
    {{- $repo := index .root.repositories $imageDef.repository -}}
    {{- if (not $repo) -}}
      {{ fail (printf "Unknown repository '%s' for image '%s'" $imageDef.repository .name) }}
    {{- end -}}

    {{- if (not $imageDef.image) }}
      {{ fail (printf "Image definition for '%s' must have an 'image' field" .name) }}
    {{- end -}}
    {{- $defaultImage := printf "%s%s" ($repo.pathPrefix | default "") $imageDef.image -}}
    {{- if $repo.hostname }}
      {{- $defaultImage = printf "%s/%s" $repo.hostname $defaultImage -}}
    {{- end }}
    {{- $defaultImage -}}
  {{- end }}
{{- end -}}
{{- end -}}

{{/* 
    aistor.resolveImagePullPolicy resolves the given name into a pull policy.
    - name: the name of the image in $.images
    - root: the root object
    - val: the value of the pull policy in the current object
*/}}
{{- define "aistor.resolveImagePullPolicy" -}}
{{- $emptyObj := ("{}" | fromJson) -}}
{{- $images := .root.images | default $emptyObj -}}
{{- $imageDef := index $images .name -}}
{{- if kindIs "map" $imageDef }}
{{- $repositoryName := $imageDef.repository | default "" -}}
{{- $repo := index .root.repositories $repositoryName | default $emptyObj -}}
{{- .val.imagePullPolicy | default $repo.imagePullPolicy | default "" -}}
{{- else -}}
{{- "" -}}
{{- end -}}
{{- end -}}

{{/* 
    aistor.resolveImagePullSecrets resolves the given name into a pull secrets.
    - name: the name of the image in $.images
    - root: the root object
*/}}
{{- define "aistor.resolveImagePullSecrets" -}}
{{- $emptyObj := ("{}" | fromJson) -}}
{{- $emptyArray := ("[]" | fromJsonArray) -}}
{{- $images := .root.images | default $emptyObj -}}
{{- $imageDef := index $images .name -}}
{{- if kindIs "map" $imageDef }}
{{- $repositoryName := $imageDef.repository | default "" -}}
{{- $repo := index .root.repositories $repositoryName | default $emptyObj -}}
{{- $repo.imagePullSecrets | default $emptyArray | toJson -}}
{{- else -}}
{{- $emptyArray | toJson -}}
{{- end -}}
{{- end -}}

{{/* 
    aistor.resolveEnv resolves the given name into the environment.
    - name: the name of the image in $.images
    - root: the root object
*/}}
{{- define "aistor.resolveEnv" -}}
{{- $emptyObj := ("{}" | fromJson) -}}
{{- $emptyArray := ("[]" | fromJsonArray) -}}
{{- $images := .root.images | default $emptyObj -}}
{{- $imageDef := index $images .name | default $emptyObj -}}
{{- if kindIs "map" $imageDef }}
{{- $imageDef.env | default $emptyArray | toJson -}}
{{- else -}}
{{- $emptyArray | toJson -}}
{{- end -}}
{{- end -}}

{{/*
    aistor.isOpenShift returns whether the current cluster is OpenShift.
*/}}
{{- define "aistor.isOpenShift" -}}
{{- if ($.Values.global.forceOpenShift | default (.Capabilities.APIVersions.Has "security.openshift.io/v1/SecurityContextConstraints")) -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
    aistor.operatorEnabled returns whether the operator is enabled.
*/}}
{{- define "aistor.operatorEnabled" -}}
{{- $emptyObj := "{}" | fromJson -}}
{{- $operators := .root.operators | default $emptyObj -}}
{{- $opValue := index $operators .operator | default $emptyObj }}
{{- if not $opValue.disabled }}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
    aistor.operators returns the default information for all operators
*/}}
{{- define "aistor.operators" -}}
minkms:
  images: ["minkms","minkms-sidecar"]
{{- end -}}
