from distutils.core import setup

setup(
    name="bikidata",
    version="0.1",
    description="Queries over Wikidata dumps",
    author="Etienne Posthumus",
    author_email="ep@epoz.org",
    url="https://github.com/ISE-FIZKarlsruhe/bikidata",
    py_modules=["bikidata"],
    install_requires=["duckdb"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
