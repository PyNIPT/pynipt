# PyNIPT (Python NeuroImage Pipeline Tool)
### Version: 0.1

#### Common features:
- 
- Provide Continuity of analysis working flow
- While the BIDS became a most prominent standard for organizing dataset, as the BIDS original purpose stated to make it handy for processing data,
we developed a pipeline scripting platform that can be used in Python interpreter environment with easy-to-use and easy-to-manage features to support the neuroscince community in terms of 
reproducibility of the analysis.
- PyNIPT provides pipeline scripting platform for Jupyter Project environment to help preclinical neuroimaging researchers, 
- especially who are not familiar with bash scripting or MATLAB coding.   
- The key features of this module are 
    1) execute shell command or python function over the BIDS dataset. So unless the dataset organized as BIDS, any command could be used.
    2) Own scheduler for parallel execution (through threading).
    3) Can be running on None-blocking manner(background processing) in jupyter notebook (so that while pipeline running, you can investigate the progression or data in same notebook or interpreter without GIL).
    5) Plugin which a simple python module can be shared through python naive repository Pypi
    6) freedom from adding up prefix and suffix among the process, (all processing steps handled as isolated folder while preserve the filename (BIDS standard, which is already long enough))
    7) Centralized Logging
- To minimize its dependency, this module only designed as scripting platform, so the command line interfaces and pipelines are not embedded in main source code. Instead, those will be imported as plugin.

#### Plugin = pipelines and interfaces, Extensions = helper function designed for specific analysis tasks.

- ***Dependency:***
    - paralexe, tdqm, pandas

- ***Compatibility:*** 
    - Python > 3.7.5
    - Brain imaging data structure (http://bids.neuroimaging.io)
    - Jupyter notebook environment (https://jupyter.org)

- ***ChangeLog:***
    - v0.1.1    - improve stability and plugin interface.
    - v0.1.0    - major stability and user interface improved.
    - v0.0.2    - stability improvement, realtime update of progress bar.
    - v0.0.1    - prerelease version.
    
#### Few Tips need to be documented
- Regex patterns using in this module
    - This module use regular expression to search specific filename without extension, 
    so the extension must be provided as separate filter key.
- Filter key
    - Dataclass specific keys
        - dataset path (idx:0): subjects, datatypes
        - working path (idx:1): pipelines, steps
        - results path (idx:2): pipelines, reports
        - masking path (idx:3): subjects, datatypes
        - temporary    (idx:4): pipelines, steps
    - File specific keys
        - regex: regex pattern for filename
        - ext: file extension
- Output filename specification
    - the output can be modified with prefix, suffix and modifier,
    - the modifier is the dictionary which the key to find keyword and value as the keyword for replacing the original
    - Or in case of reporting purpose (which the input_method=1), single string can be use as output filename
    - Using prefix, suffix, and/or modifier will result in the change of output filename.

- Output checker
    - output checker works to check if the specified files are generated.
    - the prefix and suffix here is only required when the subprocess you are running is modifying the filename
    - Usually, if the subprocess not adding any of prefix or suffix on the file they are generated, 
    - this option does not required.
    
#### Tutorials
- How to handle and parse dataset
- How to execute processing though dataset
- How to set and run pipeline
- How to debug active pipeline
- Introduction of Plugin structure
- How to build interface plugin
- How to build pipelines plugin
- How to share your plugin package
    
#### License

PyNIPT is licensed under the term of the GNU GENERAL PUBLIC LICENSE Version 3

#### Authors

The main author of **PyNIT project** is SungHo Lee (shlee@unc.edu). Please join us if you want to contribute this project.

#### Contributors
