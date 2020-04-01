#!/usr/scripts/env python
"""
PyNIPT (Python NeuroImaging Pipeline Tools)
"""
from distutils.core import setup
# from distutils.extension import Extension
from setuptools import find_packages
# from Cython.Distutils import build_ext
import re, io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('pynipt/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

__author__ = 'SungHo Lee'
__email__ = 'shlee@unc.edu'

setup(name='PyNIPT',
      version=__version__,
      description='Python NeuroImaging Pipeline Tools',
      author=__author__,
      author_email=__email__,
      url=None,
      license='GNLv3',
      packages=find_packages(),
      install_requires=[
          'numpy',
          'miresi',
          'paralexe',
          'pandas',
          'tqdm',
          'psutil',
                       ],
      # scripts=['',
      #         ],
      classifiers=[
            # How mature is this project? Common values are
            #  3 - Alpha
            #  4 - Beta
            #  5 - Production/Stable
            'Development Status :: 3 - Alpha',

            # Indicate who your project is intended for
            'Framework :: Jupyter',
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'Natural Language :: English',

            # Specify the Python version you support here. In particular, ensure
            # that you indicate whether you support Python 2, Python 3 or both
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.7',
      ],
      keywords = 'Python NeuroImaging Pipeline Tools'

     )

