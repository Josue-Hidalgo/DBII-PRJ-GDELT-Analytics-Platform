#!/bin/bash
set -e

# Si se pasa un comando explícito (como hace docker-compose), ejecutarlo directamente
exec "$@"
