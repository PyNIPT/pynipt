from pynipt import PipelineBuilder


class UNCCH_CAMRI(PipelineBuilder):
    def __init__(self, interface,
                 # User defined arguments
                 # -- start -- #
                 template_path=None, anat='anat', func='func',
                 tr=2, tpattern='altplus',
                 regex=None,
                 fwhm=0.5, mask_path=None,
                 hrf_model=None, hrf_parameters=None, stim_onsets=None,
                 step_idx=None, step_tag=None,
                 # --  end  -- #
                 ):
        """
        Standard fMRI pipeline package for the University of North Carolina at Chapel Hill,
        to use for the data analysis services in Center of Animal MRI (CAMRI)
        Author  : SungHo Lee(shlee@unc.edu)
        Revised :
            ver.1: Dec.11st.2017
            ver.2: Mar.7th.2019

        Keyword Args: - listed based on each steps
            - 01_EmptyMaskPreparation
            anat(str):          datatype for anatomical image (default='anat')
            func(str):          datatype for functional image (default='func')
            tr(int):            the repetition time of EPI data
            tpattern(str):      slice order of image
                alt+z = altplus   = alternating in the plus direction
                alt+z2            = alternating, starting at slice #1 instead of #0
                alt-z = altminus  = alternating in the minus direction
                alt-z2            = alternating, starting at slice #nz-2 instead of #nz-1
                seq+z = seqplus   = sequential in the plus direction
                seq-z = seqminus  = sequential in the minus direction

            - 02_CorePreprocessing
            template_path(str): absolute path of brain template image (default=None)

            - 03_TaskBaseAnalysis
            regex(str):             Regular express pattern of filename to select dataset
            mask_path(str):         path of brain mask image
            fwhm(int or float):     full width half maximum value for smoothing
            hrf_model(str):         Hemodynamic Response Function (HRF) according to the 3dDeconvolve command in Afni
            hrf_parameters(list):   parameter values for the HRF
            stim_onset(list):       stimulation onset times
            step_idx(idx):          step_index to classify the step with other when apply multiple
            step_tag(str):          suffix tag to classify the step with other when apply multiple
        """
        super(UNCCH_CAMRI, self).__init__(interface)
        # User defined attributes for storing arguments
        # -- start -- #
        # 01_EmptyMaskPreparation
        self.anat = anat
        self.func = func
        self.tr = tr
        self.tpattern = tpattern

        # 02_CorePreprocessing
        self.template_path = template_path

        # 03_TaskBaseAnalysis
        self.regex = regex
        self.mask_path = mask_path
        self.fwhm = fwhm
        self.hrf_model = hrf_model
        self.hrf_parameters = hrf_parameters
        self.stim_onsets = stim_onsets
        self.step_idx = step_idx
        self.step_tag = step_tag
        # --  end  -- #

    def pipe_01_EmptyMaskPreparation(self):
        """
        The dataset will be first slice timing corrected, and average intensity map of functional image
        will be calculated on motion corrected data. In the end of this pipeline, empty image file will be
        generated in the masking path as a place holder of mask with '_mask' suffix.

        If the anatomical data were inputted, the empty mask files will be generated as well.
        """
        # Series of user defined interface commands to executed for the pipeline
        # -- start -- #
        self.interface.afni_SliceTimingCorrection(input_path=self.func,
                                                  tr=self.tr, tpattern=self.tpattern,
                                                  step_idx=1, sub_code=0)
        self.interface.afni_MotionCorrection(input_path='010', file_idx=0,
                                             fourier=True, verbose=True, mparam=False,
                                             step_idx=2, sub_code='A', suffix='base')
        self.interface.afni_MeanImageCalc(input_path='010',
                                          step_idx=1, sub_code='A')
        self.interface.afni_MakeEmptyMask(input_path='01A', file_idx=0,
                                          step_idx=1, sub_code='B', suffix=self.func)
        if self.anat is not None:
            self.interface.afni_MakeEmptyMask(input_path=self.anat, file_idx=0,
                                              step_idx=1, sub_code='C', suffix=self.anat)


    def pipe_02_CorePreprocessing(self):
        """
        Prior to run this pipeline, template_path argument need to be inputted.

        All the dataset will be motion corrected, and skull stripping will be applied.
        If the anatomical data are inputted, then functional data will be co-registered into
        anatomical space using affine registration.

        Finally, the functional data will be normalized into template space using ANTs SyN non-linear
        registration.
        """
        # Series of user defined interface commands to executed for the pipeline
        # -- start -- #

        # Check if the brain template image is exist in given path
        if self.template_path is None:
            raise Exception('No brain template image is provided.')
        else:
            if not self.interface.msi.path.exists(self.template_path):
                raise Exception('No brain template image found on given path.')

        self.interface.afni_MotionCorrection(input_path='010', base='02A',
                                             fourier=True, verbose=True, mparam=True,
                                             step_idx=2, sub_code=0, suffix=self.func)
        self.interface.afni_SkullStripping(input_path='01A', mask_path='01B', file_idx=0,
                                           step_idx=3, sub_code='A', suffix='mean{}'.format(self.func))
        if self.anat is not None:
            # if anatomy dataset is provided, then co-registration process will be applied
            self.interface.afni_SkullStripping(input_path=self.anat, mask_path='01C',
                                               step_idx=3, sub_code='B', suffix=self.anat)
            self.interface.ants_N4BiasFieldCorrection(input_path='03A', file_idx=0,
                                                      step_idx=3, sub_code='C',
                                                      suffix='mean{}'.format(self.func))
            self.interface.ants_N4BiasFieldCorrection(input_path='03B', file_idx=0,
                                                      step_idx=3, sub_code='D',
                                                      suffix=self.anat)
            self.interface.afni_Coregistration(input_path='03C', ref_path='03D', file_idx=0,
                                               step_idx=3, sub_code='E',
                                               suffix='mean{}'.format(self.func))
            self.interface.ants_SpatialNorm(input_path='03D', ref_path=self.template_path,
                                            step_idx=4, sub_code='A',
                                            suffix=self.anat)
            self.interface.afni_SkullStripping(input_path='020', mask_path='01B',
                                               step_idx=3, sub_code='F',
                                               suffix=self.func)
            self.interface.afni_ApplyTransform(input_path='03F', ref_path='03E',
                                               step_idx=3, sub_code=0,
                                               suffix=self.func)
        else:
            self.interface.afni_SkullStripping(input_path='020', mask_path='01B',
                                               step_idx=3, sub_code=0,
                                               suffix=self.func)
            self.interface.ants_SpatialNorm(input_path='03C', ref_path=self.template_path,
                                            step_idx=4, sub_code='A',
                                            suffix=self.anat)

        self.interface.ants_ApplySpatialNorm(input_path='030', ref_path='04A',
                                             step_idx=4, sub_code=0,
                                             suffix=self.func)
        # --  end  -- #

    def pipe_03_TaskBasedAnalysis(self):
        """
        The normalized data will be scaled to have mean value of 100 for each voxel followed by the spacial smoothing
        to
        """
        # Series of user defined interface commands to executed for the pipeline
        # -- start -- #
        self.interface.afni_Scailing(input_path='040', mask_path=self.mask_path,
                                     mean=100, max=200, step_idx=self.step_idx, sub_code='A',
                                     suffix=self.step_tag)
        self.interface.afni_BlurToFWHM(input_path='{}A'.format(str(self.step_idx).zfill(2)),
                                       fwhm=self.fwhm,
                                       step_idx=self.step_idx, sub_code='B', suffix=self.step_tag)
        self.interface.afni_Deconvolution(input_path='{}B'.format(str(self.step_idx).zfill(2)),
                                          mask_path=self.mask_path,
                                          regex=self.regex,
                                          onset_time=self.stim_onsets, model=self.hrf_model,
                                          parameters=self.hrf_parameters,
                                          step_idx=self.step_idx, sub_code=0, suffix=self.step_tag)
        # --  end  -- #