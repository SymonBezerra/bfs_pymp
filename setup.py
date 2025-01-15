from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        'graph',
        ['graph.pyx'],
        language='c++'
    ),
    Extension(
        'thread',
        ['thread.pyx'],
        language='c++'
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': '3'
        }
    )
)