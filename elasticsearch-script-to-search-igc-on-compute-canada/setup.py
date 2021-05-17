import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="igc_searcher", 
    version="0.0.1",
    author="Alexandra Lilia Nastevski",
    description="A package to extract lemmas and tokens from the Icelandic Gigaword Corpus",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)
