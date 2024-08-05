#!/usr/bin/env python
# Wrapper to start jpype and run a script like it would under jython.
import sys
import os, fnmatch
import jpype
import jpype.imports
from jpype.types import *

# https://stackoverflow.com/a/41658338
def execfile(filepath, globals=None, locals=None):
    if globals is None:
        globals = {}
    globals.update({
        "__file__": filepath,
        "__name__": "__main__",
    })
    with open(filepath, 'rb') as file:
        exec(compile(file.read(), filepath, 'exec'), globals, locals)

# https://stackoverflow.com/a/1724723
#def find(pattern, path):
#    result = []
#    for root, dirs, files in os.walk(path):
#        for name in files:
#            if fnmatch.fnmatch(name, pattern):
#                result.append(os.path.join(root, name))
#    return result

#sys.path.append(find('jython-db-api-*.jar', os.path.dirname(__file__))[0])

sys.path.append(os.path.join(os.path.dirname(__file__), 'hec_jython'))

if __name__ == '__main__':
    print("Starting JVM")
    jpype.startJVM(f"-Xms{os.environ.get('JAVA_XMS', '256m')}", f"-Xmx{os.environ.get('JAVA_XMX', '256m')}")

    # Load argument as script, then pass along remaining arguments
    try:
        if len(sys.argv) == 1:
            # show python interactive console, as if a script wasn't passed
            import code
            code.interact()
        else:
            report_file = sys.argv[1]
            # Compile the report, so source and line number information can be reported to the user
            execfile(report_file, globals())

    finally:
        # Report execution as finished, shut down the JVM
        if jpype.isJVMStarted():
            print("Shutting down JVM")
            jpype.shutdownJVM()

