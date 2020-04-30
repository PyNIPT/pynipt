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
__url__ = 'https://github.com/dvm-shlee/pynipt'

setup(name='PyNIPT',
      version=__version__,
      description='Python NeuroImaging Pipeline Tools',
      author=__author__,
      author_email=__email__,
      url=__url__,
      license='GNLv3',
      packages=find_packages(),
      install_requires=[
          'numpy>=1.18.0',
          'pandas>=1.0.0',
          'tqdm>=4.40.0',
          'psutil>=5.5.0',
          'paralexe>=0.0.3',
          'shleeh>=0.0.3'
                       ],
      # scripts=['',
      #         ],
      classifiers=[
            'Development Status :: 4 - Beta',
            'Framework :: Jupyter',
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'Natural Language :: English',
            'Programming Language :: Python :: 3.7',
      ],
      keywords='Python NeuroImaging Pipeline Tools'

     )

