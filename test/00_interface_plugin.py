from pynipt import Processor, InterfaceBuilder


class Interface(Processor):
    """command line interface example
    """
    def __init__(self, *args, **kwargs):
        super(Interface, self).__init__(*args, **kwargs)

    def afni_MeanImageCalc(self, input_path,
                           range=None, file_idx=0, img_ext='nii.gz',
                           step_idx=None, sub_code=None, suffix=None):
        """Calculate mean intensity image using 3dTstat

        Args:
            input_path(str):    datatype or stepcode of input data
            range(list):        range for averaging (default=None)
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='MeanImageCalculation',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0,
                      idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='output')
        cmd = ["3dTstat -prefix *[output] -mean"]
        if range is not None:
            if isinstance(range, list) and len(range) is 2:
                start, end = range
                if isinstance(start, int) and isinstance(end, int):
                    itf.set_var(label='start', value=start)
                    itf.set_var(label='end', value=end)
                else:
                    self.logging('warn', 'incorrect range values.')
            else:
                self.logging('warn', 'incorrect range values.')
            cmd.append("*[input]'[*[start]..*[end]]'")
        else:
            cmd.append("*[input]")
        itf.set_cmd(' '.join(cmd))
        itf.check_output()
        itf.run()

    def afni_SliceTimingCorrection(self, input_path,
                                   tr=None, tpattern=None, img_ext='nii.gz',
                                   step_idx=None, sub_code=None, suffix=None):
        """Correct slice timing using afni's 3dTshift command

        Args:
            input_path(str):    datatype or stepcode of input data
            tr(int or float):   sampling rate
            tpattern(str):      slice timing pattern available in 3dTshift
                                (e.g. altplus, altminus, seqplut, seqminus)
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='SliceTimingCorrection',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='output')
        cmd = ['3dTshift -prefix *[output]']
        if tr is not None:
            itf.set_var(label='tr', value=tr)
            cmd.append('-TR *[tr]')
        if tpattern is not None:
            itf.set_var(label='tpattern', value=tpattern)
            cmd.append('-tpattern *[tpattern]')
        cmd.append('*[input]')
        itf.set_cmd(' '.join(cmd))
        itf.check_output()
        itf.run()

    def afni_MotionCorrection(self, input_path,
                              base=0, fourier=True, verbose=True,
                              mparam=True, file_idx=None, img_ext='nii.gz',
                              step_idx=None, sub_code=None, suffix=None):
        """Correct head motion using afni's 3dvolreg command

        Args:
            input_path(str):    datatype or stepcode of input data
            step_idx(int):      stepcode index (positive integer lower than 99)
            base(int):
            fourior(bool):
            verbose(bool):
            mparam(bool):
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='MotionCorrection',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='output')

        cmd = ["3dvolreg -prefix *[output]"]
        if mparam is True:
            itf.set_output(label='mparam', ext='1D')
            cmd.append("-1Dfile *[mparam]")
        if fourier is True:
            cmd.append("-Fourier")
        if verbose is True:
            cmd.append("-verbose")
        if isinstance(base, int):
            # use frame number for the reference
            itf.set_var(label='base', value=base)
        elif isinstance(base, str):
            # use input_path for the reference
            itf.set_static_input(label='base', input_path=base, idx=0,
                                 filter_dict=dict(ext=img_ext))
        cmd.append('-base *[base] *[input]')

        itf.set_cmd(' '.join(cmd))
        itf.check_output()
        itf.run()

    def afni_MakeEmptyMask(self, input_path, file_idx=None, img_ext='nii.gz',
                          step_idx=None, sub_code=None, suffix=None):
        """ make empty mask correspond to the input data at masking path

        Args:
            input_path(str):    datatype or stepcode of input data
            file_idx:
            img_ext:
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='MakeEmptyMask', mode='masking',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='mask', suffix='mask')
        itf.set_output(label='copy')
        itf.set_cmd("3dcalc -prefix *[mask] -expr 'a*0' -a *[input]")
        itf.set_cmd("cp *[input] *[copy]")
        itf.check_output(label='mask')
        itf.run()
