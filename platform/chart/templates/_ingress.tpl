{{/*
  The one list of public surfaces (#24): both the Ingress template and the HTTPRoute template
  read it, so a component published by one install shape is published by the other.

  Each entry: name, service, port, sub (the subdomain), enabled, and optionally paths.
  A path entry is either a string (Prefix) or a mapping of `path` + `pathType`.
*/}}
{{- define "stardelt.ingress.services" -}}
{{- toYaml (list
      (dict "name" "nova"       "service" "nova"               "port" 8080 "sub" ""           "enabled" .Values.nova.enabled)
      (dict "name" "trino"      "service" "trino"              "port" 8080 "sub" "trino"      "enabled" .Values.trino.enabled "paths" (list "/v1/statement" "/oauth2"))
      (dict "name" "lakekeeper" "service" "lakekeeper"         "port" 8181 "sub" "lakekeeper" "enabled" .Values.lakekeeper.enabled)
      (dict "name" "superset"   "service" "superset"           "port" 8088 "sub" "superset"   "enabled" .Values.superset.enabled)
      (dict "name" "airflow"    "service" "airflow-api-server" "port" 8080 "sub" "airflow"    "enabled" .Values.airflow.enabled)
      (dict "name" "jupyterhub" "service" "proxy-public"       "port" 80   "sub" "jupyter"    "enabled" .Values.jupyterhub.enabled)
      (dict "name" "keycloak"   "service" "keycloak"           "port" 8080 "sub" "id"         "enabled" (eq (include "stardelt.identityMode" .) "platform") "paths" (list
        (dict "path" "/realms/stardelt"        "pathType" "Prefix")
        (dict "path" "/resources"              "pathType" "Prefix")
        (dict "path" "/robots.txt"             "pathType" "Prefix")
        (dict "path" "/admin/stardelt"         "pathType" "Prefix")
        (dict "path" "/admin/realms/stardelt"  "pathType" "Prefix")
        (dict "path" "/admin/serverinfo"       "pathType" "Prefix"))))
-}}
{{- end -}}
