# PyNIPT (Python NeuroImage Pipeline Tool)
### Version: 0.1.0

#### Common features:
- PyNIPT is a part of **PyNIT project** to provide pipeline scripting tool for helping researchers in preclinical neuroimaging field, especially who are not familiar with bash scripting or MATLAB coding.   
- This module provides easy to use scripting environment to execute linux-based neuroimage processing command line tools, such as FSL, AFNI, and ANTs, along the BIDS dataset.
- To minimize its dependency, this module only designed as scripting platform, so the command line interfaces and pipelines are not embedded in main source code. Instead, those will be imported as plugin.
    - *More detail about the plugin script, please refer my gist (https://gist.github.com/dvm-shlee/52aa93427b98d1d7099d3736c78bfeb4)*

- ***Dependency:***
    - paralexe, tdqm, pandas

- ***Compatibility:*** 
    - Python > 2.7 or > 3.7
    - Brain imaging data structure (http://bids.neuroimaging.io)
    - Jupyter notebook environment (https://jupyter.org)

- ***ChangeLog:***
    - v0.1.0    - major stability and user interface improved.
    - v0.0.2    - stability improvement, realtime update of progress bar.
    - v0.0.1    - prerelease version.
    
#### License

PyNIPT is licensed under the term of the GNU GENERAL PUBLIC LICENSE Version 3

#### Author

The main author of **PyNIT project** is SungHo Lee (shlee@unc.edu). Please join us if you want to contribute this project.