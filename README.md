# PDF Generation
This project will build a PDF using iText or OpenPDF in a docker container.
Intended for final deployment in the cloud.

# Building
To build the docker container, use the included Dockerfile.

`docker build . -t itext`

# Building a PDF
You can build a PDF using either jython (as originally used in USACE), or using jpype as the java interop. Either should produce identical outputs.
The default is jython for compatibility.

Jython:
`docker run -it itext /input/<inputscript.py>`

jpype:
`docker run -it itext jpype /input/<inputscript.py>`

An example has been included, and can be run by placing the `input` folder into the root of the project before building the container.
Once built, run the report with:

`docker run -it itext /input/NWD_River/NWD_Daily_River_Bulletin.py`

To see the output, copy it out of the container:
`docker cp $(docker container ls -q):/output/NWD_Daily_River_Bulletin.pdf .`
