# Selvedge development infrastructure.
# Usage: tilt up

docker_compose("docker-compose.yml")
dc_resource("postgres", labels=["infrastructure"])
dc_resource("jaeger", labels=["tracing"], links=["http://localhost:16686"])
