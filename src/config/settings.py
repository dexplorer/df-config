import confuse
import logging

class ConfigParms:
    env = ''
    app_root_dir = ''
    config = confuse.Configuration("dist_app", __name__)

    # Define config variables at module scope
    cfg_file_path = ''
    log_file_path = ''
    data_in_file_path = ''
    data_out_file_path = ''
    sql_script_file_path = ''
    img_out_file_path = ''
    hive_warehouse_path = ''
    s3_prefix = ''
    s3_bucket = ''
    s3_region = ''

    @classmethod
    def set_config_file(cls):
        try:
            if cls.env == "prod":
                cls.config.set_file(f"{cls.app_root_dir}/cfg/config.yaml")
            elif cls.env == "qa":
                cls.config.set_file(f"{cls.app_root_dir}/cfg/config_qa.yaml")
            elif cls.env == "dev":
                cls.config.set_file(f"{cls.app_root_dir}/cfg/config_dev.yaml")
            else:
                raise RuntimeError(
                    "Environment is invalid. Accepted values are prod / qa / dev ."
                )
        except RuntimeError as error:
            logging.error(error)
            raise

    @classmethod
    def load_config(cls):
        cls.set_config_file()
        
        cfg = cls.config["CONFIG"].get()
        logging.info(cfg)
        # print(cfg)

        cls.cfg_file_path = f"{cls.resolve_app_path(cfg['APP_CONFIG_DIR'])}"
        cls.log_file_path = f"{cls.resolve_app_path(cfg['APP_LOG_DIR'])}"
        cls.data_in_file_path = f"{cls.resolve_app_path(cfg['APP_DATA_IN_DIR'])}"
        cls.data_out_file_path = f"{cls.resolve_app_path(cfg['APP_DATA_OUT_DIR'])}"
        cls.sql_script_file_path = f"{cls.resolve_app_path(cfg['APP_SQL_SCRIPT_DIR'])}"
        cls.img_out_file_path = f"{cls.resolve_app_path(cfg['APP_IMG_OUT_DIR'])}"
        cls.hive_warehouse_path = f"{cls.resolve_app_path(cfg['HIVE_WAREHOUSE_DIR'])}"

    @classmethod
    def resolve_app_path(cls, rel_path):
        cfg = cls.config["CONFIG"].get()
        logging.info(cfg)

        resolved_app_path = (
            rel_path.replace("APP_CONFIG_DIR", cfg["APP_CONFIG_DIR"])
            .replace("APP_LOG_DIR", cfg["APP_LOG_DIR"])
            .replace("APP_DATA_IN_DIR", cfg["APP_DATA_IN_DIR"])
            .replace("APP_DATA_OUT_DIR", cfg["APP_DATA_OUT_DIR"])
            .replace("APP_SQL_SCRIPT_DIR", cfg["APP_SQL_SCRIPT_DIR"])
            .replace("APP_IMG_OUT_DIR", cfg["APP_IMG_OUT_DIR"])
            .replace("APP_ROOT_DIR", cls.app_root_dir)
        )
        return resolved_app_path

    @classmethod
    def load_aws_config(cls, aws_iam_user_name: str):
        cls.set_config_file()

        if user := aws_iam_user_name.upper():
            aws_user_cfg = cls.config["AWS_USER_CONFIG"][user].get()
            logging.info(aws_user_cfg)
            # print(aws_user_cfg)

            cls.s3_prefix = aws_user_cfg["S3_PREFIX"]
            cls.s3_bucket = aws_user_cfg["S3_BUCKET"]
            cls.s3_region = aws_user_cfg["S3_REGION"]
