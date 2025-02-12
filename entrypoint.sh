#!/bin/sh
export REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-bundle.trust.crt"

# Fix permissions of volumes
if [ "${FIX_PERMISSIONS:-0}" -gt 0 ]; then
	chown $UID:$GID /output
fi

if [ "$1" = "jpype" ]; then
	shift
	exec /opt/pdf-gen/jpype "$@"
else
	[ -n "$VERBOSE" ] && [ $VERBOSE -gt 0 ] && set>&2 &&
		echo /opt/pdf-gen/jython "$@">&2
	exec /opt/pdf-gen/jython "$@"
fi
