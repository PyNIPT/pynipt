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
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
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
        itf.check_output()              # default label='output'
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
            img_ext(str):       file extension (default='nii.gz')
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
        itf.check_output()              # default label='output'
        itf.run()

    def afni_MotionCorrection(self, input_path,
                              base=0, fourier=True, verbose=True,
                              mparam=True, file_idx=None, img_ext='nii.gz',
                              step_idx=None, sub_code=None, suffix=None):
        """Correct head motion using afni's 3dvolreg command

        Args:
            input_path(str):    datatype or stepcode of input data
            step_idx(int):      stepcode index (positive integer lower than 99)
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            base(int or str):   reference image
            fourior(bool):      Fourior option on for 3dvolreg
            verbose(bool):      if True, print out all processing messages on STERR.log
            mparam(bool):       if True, generate motion parameter file(1D) using same filename
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
        itf.check_output()              # default label='output'
        itf.run()

    def afni_MakeEmptyMask(self, input_path, file_idx=None, img_ext='nii.gz',
                          step_idx=None, sub_code=None, suffix=None):
        """ make empty mask correspond to the input data at masking path

        Args:
            input_path(str):    datatype or stepcode of input data
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='MakeEmptyMask', mode='masking',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='mask', suffix='_mask')
        itf.set_output(label='copy')
        itf.set_cmd("3dcalc -prefix *[mask] -expr 'a*0' -a *[input]")
        itf.set_cmd("cp *[input] *[copy]")
        itf.check_output(label='mask')
        itf.run()

    def afni_SkullStripping(self, input_path, mask_path,
                            file_idx=None, img_ext='nii.gz',
                            step_idx=None, sub_code=None, suffix=None):
        """ stripping the skull using brain mask

        Args:
            input_path(str):    datatype or stepcode of input data
            mask_path(str):     stepcode of mask_path
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='SkullStripping', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_static_input(label='mask', input_path=mask_path,
                             idx=0, mask=True, filter_dict=dict(regex=r'.*_mask$',ext=img_ext))
        itf.set_output(label='output')
        itf.set_cmd("3dcalc -prefix *[output] -expr 'a*step(b)' -a *[input] -b *[mask]")
        itf.check_output()              # default label='output'
        itf.run()

    def ants_N4BiasFieldCorrection(self, input_path,
                                   file_idx=None, img_ext='nii.gz',
                                   step_idx=None, sub_code=None, suffix=None):
        """ correcting bias field using N4 algorithm of ants package

        Args:
            input_path(str):    datatype or stepcode of input data
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='N4BiasFieldCorrection', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_output(label='output')
        itf.set_cmd("N4BiasFieldCorrection -i *[input] -o *[output]")
        itf.check_output()              # default label='output'
        itf.run()

    def afni_Coregistration(self, input_path, ref_path,
                            file_idx=None, img_ext='nii.gz',
                            step_idx=None, sub_code=None, suffix=None):
        """ realign the functional image into anatomical image using 3dAllineate command of
        afni package.

        Args:
            input_path(str):    datatype or stepcode of input data
            ref_path(str):      stepcode of reference data
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='Coregistration', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_static_input(label='ref', input_path=ref_path,
                             idx=0, filter_dict=dict(ext=img_ext))
        itf.set_output(label='output')
        itf.set_output(label='tfmat', ext='aff12.1D')
        itf.set_cmd("3dAllineate -prefix *[output] -onepass -EPI -base *[ref] -cmass+xy "
                    "-1Dmatrix_save *[tfmat] *[input]")
        itf.check_output()              # default label='output'
        itf.run()

    def afni_ApplyTransform(self, input_path, ref_path,
                            file_idx=None, img_ext='nii.gz',
                            step_idx=None, sub_code=None, suffix=None):
        """ apply the transform matrix that acquired from 3dAllineate command
        along all input data using 3dAllineate command of afni package.

        Args:
            input_path(str):    datatype or stepcode of input data
            ref_path(str):      stepcode of reference data
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='ApplyTransform', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_static_input(label='ref', input_path=ref_path,
                             idx=0, filter_dict=dict(ext=img_ext))
        itf.set_static_input(label='tfmat', input_path=ref_path,
                             idx=0, filter_dict=dict(ext='aff12.1D'))
        itf.set_output(label='output')
        itf.set_cmd("3dAllineate -prefix *[output] -master *[ref] -1Dmatrix_apply *[tfmat] *[input]")
        itf.check_output()  # default label='output'
        itf.run()

    def ants_SpatialNorm(self, input_path, ref_path,
                         file_idx=None, img_ext='nii.gz',
                         step_idx=None, sub_code=None, suffix=None):
        """ realign subject brain image into standard space using antsRegistrationSyN.sh command
        of ants package

        Args:
            input_path(str):    datatype or stepcode of input data
            ref_path(str):      path for brain template image
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self, n_threads=1)
        itf.init_step(title='SpatialNorm', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0, idx=file_idx,
                      filter_dict=dict(ext=img_ext))
        itf.set_var(label='ref', value=ref_path)
        itf.set_var(label='thread', value=self._n_threads)
        itf.set_output(label='output', suffix='_', ext=False)
        itf.set_cmd("antsRegistrationSyN.sh -f *[ref] -m *[input] -o *[output] -n *[thread]")
        itf.check_output(suffix='Warped', ext='nii.gz')
        itf.run()

    def ants_ApplySpatialNorm(self, input_path, ref_path,
                              file_idx=None, img_ext='nii.gz',
                              step_idx=None, sub_code=None, suffix=None):
        """ apply transform matrix generated by antsRegistrationSyN.sh along other images
        using WarpTimeSeriesImageMultiTransform command of ants package

        Args:
            input_path(str):    datatype or stepcode of input data
            ref_path(str):      path for brain template image
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='ApplySpatialNorm', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, method=0,
                      idx=file_idx, filter_dict=dict(ext=img_ext))
        itf.set_static_input(label='base', input_path=ref_path,
                             idx=0, filter_dict=dict(regex=r'.*_Warped$', ext='nii.gz'))
        itf.set_static_input(label='tfmorph', input_path=ref_path,
                             idx=0, filter_dict=dict(regex=r'.*_1Warp$', ext='nii.gz'))
        itf.set_static_input(label='tfmat', input_path=ref_path,
                             idx=0, filter_dict=dict(ext='mat'))
        itf.set_output(label='output')
        itf.set_cmd("WarpTimeSeriesImageMultiTransform 4 *[input] *[output] -R "
                    "*[base] *[tfmorph] *[tfmat]")
        itf.check_output()
        itf.run()

    def afni_BlurInMask(self, input_path, mask_path, fwhm,
                        file_idx=None, img_ext='nii.gz',
                        step_idx=None, sub_code=None, suffix=None):
        """ FWHM based spatial gaussian smoothing using 3dBlurInMask command of Afni
        Args:
            input_path(str):    datatype or stepcode of input data
            mask_path(str):     path for brain mask image
            fwhm(float):        full width half maximum value
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='BlurInMask', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, idx=file_idx,
                      filter_dict=dict(ext=img_ext), method=0)
        itf.set_var(label='fwhm', value=str(fwhm))
        itf.set_var(label='mask', value=mask_path)
        itf.set_output(label='output')
        itf.set_cmd("3dBlurInMask -prefix *[output] -FWHM *[fwhm] -mask *[mask] *[input]")
        itf.check_output(label='output')
        itf.run()

    def afni_BlurToFWHM(self, input_path, fwhm, file_idx=None, img_ext='nii.gz',
                  step_idx=None, sub_code=None, suffix=None):
        """ FWHM based spatial gaussian smoothing using 3dmerge command of Afni
        Args:
            input_path(str):    datatype or stepcode of input data
            fwhm(float):        full width half maximum value
            file_idx(int):      index of file if the process need to be executed on a specific file
                                in session folder.
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='BlurToFWHM', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path, idx=file_idx,
                      filter_dict=dict(ext=img_ext), method=0)
        itf.set_var(label='fwhm', value=str(fwhm))
        itf.set_output(label='output')
        itf.set_cmd("3dmerge -prefix *[output] -doall -1blur_fwhm *[fwhm] *[input]")
        itf.check_output(label='output')
        itf.run()

    def afni_Scailing(self, input_path, mask_path, mean=100, max=200,
                     img_ext='nii.gz',
                     step_idx=None, sub_code=None, suffix=None):
        """ Scaling the time series dataset to have given mean in the mask
        If max value is inputted, the max value will be cut at given value.

        Args:
            input_path(str):    datatype or stepcode of input data
            mask_path(str):     path for brain mask image
            mean(int, float):   desired mean value
            max(int, float):    desired max value
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='Scaling', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        itf.set_input(label='input', input_path=input_path,
                      filter_dict=dict(ext=img_ext), method=0)
        itf.set_temporary(label='meanimg')
        itf.set_var(label='mask', value=mask_path)
        itf.set_var(label='mean', value=mean)
        if max is not None:
            itf.set_var(label='max', value=max)
            expr_block = 'c * min(*[max], a/b**[mean])'
        else:
            expr_block = 'c * min(a/b**[mean])'
        itf.set_output(label='output')
        itf.set_cmd("3dTstat -mean -prefix *[meanimg] *[input]")
        itf.set_cmd("3dcalc -a *[input] -b *[meanimg] -c *[mask] -expr '{}' -prefix *[output]".format(expr_block))
        itf.check_output(label='output')
        itf.run()

    def afni_Deconvolution(self, input_path, mask_path,
                           onset_time, model, parameters, polort=2,
                           regex=None, img_ext='nii.gz',
                           step_idx=None, sub_code=None, suffix=None):
        """ General Linear Model analysis using 3dDeconvolve of Afni package
        this interface is for use of single stimulation model only.

        Args:
            input_path(str):    datatype or stepcode of input data
            mask_path(str):     path for brain mask image
            polort(int):        polynomial regressor for detrending
            onset_time(list):   stimulation onset time, list of int (e.g. [10, 50, 90])
            model(str):         response model
            parameters(list):   parameters for response model
            regex(str):         regular express pattern to filter dataset
            img_ext(str):       file extension (default='nii.gz')
            step_idx(int):      stepcode index (positive integer lower than 99)
            sub_code(str):      sub stepcode, one character, 0 or A-Z
            suffix(str):        suffix to identify the current step
        """
        itf = InterfaceBuilder(self)
        itf.init_step(title='Deconvolve', mode='processing',
                      idx=step_idx, subcode=sub_code, suffix=suffix)
        if regex is not None:
            filter_dict = dict(regex=regex,
                               ext=img_ext)
        else:
            filter_dict = dict(ext=img_ext)
        # set input
        itf.set_input(label='input', input_path=input_path,
                      filter_dict=filter_dict, method=0)
        # set variables
        itf.set_var(label='mask', value=mask_path)
        itf.set_var(label='polort', value=polort)
        itf.set_var(label='onset_time', value=' '.join(map(str, onset_time)))
        itf.set_var(label='model', value=model)
        itf.set_var(label='parameters', value=','.join(map(str, parameters)))
        # set temporary output
        itf.set_temporary(label='bucket')
        # set main output
        itf.set_output(label='output')
        itf.set_output(label='matrix', ext=False)
        itf.set_cmd("3dDeconvolve -input *[input] -mask *[mask] "
                    "-num_stimts 1 -polort *[polort] -stim_times 1 '1D: *[onset_time]' "
                    "'*[model](*[parameters])' -stim_label 1 STIM -tout -bucket *[bucket] -x1D *[matrix]")
        itf.set_cmd("3dREMLfit -matrix *[matrix].xmat.1D -input *[input] -tout -Rbuck *[output] -verb")
        itf.check_output(label='output')
        itf.run()

    def afni_TwoSampleTtest(self, group_dict, step_idx=None, sub_code=None, suffix=None):
        """

        Args:
            step_idx:
            sub_code:
            suffix:
            **group_filters:

        Returns:

        """
        pass