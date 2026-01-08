from app.modules.utils.seed import SeedUtil

class PipelineSetup:
    @classmethod
    def setup(cls, n_seed: int):
        SeedUtil.get_instance(n_seed).seed_everything()
#        os.environ['GOOGLE_CLOUD_PROJECT'] = str_gcp_project_id

