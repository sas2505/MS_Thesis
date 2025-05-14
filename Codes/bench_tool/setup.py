from setuptools import setup, find_packages

setup(
    name="bench-tool",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "click", 
        "numpy", 
        "pandas", 
        "matplotlib",
        "pyyaml",
    ],
    entry_points={
        "console_scripts": [
            "bench-tool=bench_tool.cli:cli",
        ],
    },
    author="Md Saiful Ambia Chowdhury",
    author_email="sas2505@gmail.com",
    description="A CLI tool for benchmarking and analyzing results of DSMS outputs.",
)
