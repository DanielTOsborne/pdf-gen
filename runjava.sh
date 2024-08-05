#!/bin/sh
dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

for f in $dir/lib/*.jar; do
	CLASSPATH="$CLASSPATH:$f"
done

java -cp $CLASSPATH "$@"
