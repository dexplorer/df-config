import confuse
import logging
import os

# from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from utils import misc as ufm
from utils.enums import AppEnv, AppHostPattern, AppHostPatternEnvFile, StoragePlatform
from utils import aws_s3_io as ufa
import json


class ConfigParms:
    # Config variables from main environment file
    app_env = ""
    app_infra_platform = ""
    app_host_pattern = ""
    log_storage_platform = ""
    data_in_storage_platform = ""
    data_out_storage_platform = ""
    app_root_dir = ""
    global_cfg_dir = ""
    # Config variables from host pattern environment file
    log_handlers = []
    api_host = ""
    api_port = ""
    nas_root_dir = ""
    nas_log_dir = ""
    nas_data_in_dir = ""
    nas_data_out_dir = ""
    nas_img_out_dir = ""
    nas_datalake_dir = ""
    s3_log_bucket_uri = ""
    s3_data_in_bucket_uri = ""
    s3_data_out_bucket_uri = ""
    s3_img_out_bucket_uri = ""
    s3_datalake_bucket_uri = ""
    s3_log_bucket = ""
    s3_data_in_bucket = ""
    s3_data_out_bucket = ""
    s3_img_out_bucket = ""
    s3_datalake_bucket = ""
    s3_region = ""
    # Config variables from global config file
    config = confuse.Configuration("data_framework_app", __name__)
    app_config_dir = ""
    app_sql_script_dir = ""
    # Storage platform is NAS storage
    app_log_dir = ""
    app_data_in_dir = ""
    app_data_out_dir = ""
    app_img_out_dir = ""
    hive_warehouse_dir = ""
    # Storage platform is AWS S3 storage
    app_log_uri = ""
    app_data_in_uri = ""
    app_data_out_uri = ""
    app_img_out_uri = ""
    hive_warehouse_uri = ""
    # Storage platform agnostic output paths
    app_log_path = ""
    app_data_out_path = ""
    app_img_out_path = ""
    hive_warehouse_path = ""
    # Config variables from app config file
    app_name = ""
    # Misc
    s3_prefix = ""
    s3_bucket_uri = ""
    s3_region = ""

    @classmethod
    def __str__(cls):
        return ufm.dump_as_str(cls)

    @staticmethod
    def load_env_file(app_host_pattern: str):
        # Load the app host pattern environment variables
        env_file = AppHostPatternEnvFile[AppHostPattern(app_host_pattern).name].value

        logging.info("Finding the env file %s", env_file)
        env_file = find_dotenv(
            filename=env_file, raise_error_if_not_found=True, usecwd=True
        )
        logging.info("Loading the env file %s", env_file)
        load_dotenv(dotenv_path=env_file)
        logging.debug(os.environ)

    @staticmethod
    def load_env_file_alt():
        # Load the app host pattern environment variables
        env_file = os.path.join(
            os.environ["APP_ROOT_DIR"],
            AppHostPatternEnvFile[
                AppHostPattern(os.environ["APP_HOST_PATTERN"]).name
            ].value,
        )
        logging.info("Loading the env file %s", env_file)
        load_dotenv(env_file)
        logging.debug(os.environ)

    @classmethod
    def set_env_vars(cls):
        # Common env variables
        cls.app_env = os.environ["APP_ENV"]
        cls.app_infra_platform = os.environ["APP_INFRA_PLATFORM"]
        cls.app_host_pattern = os.environ["APP_HOST_PATTERN"]
        cls.log_storage_platform = os.environ["LOG_STORAGE_PLATFORM"]
        if cls.log_storage_platform not in StoragePlatform:
            raise RuntimeError(
                "Log storage platform is invalid. Unable to set required configurations."
            )

        cls.data_in_storage_platform = os.environ["DATA_IN_STORAGE_PLATFORM"]
        if cls.data_in_storage_platform not in StoragePlatform:
            raise RuntimeError(
                "Data in storage platform is invalid. Unable to set required configurations."
            )

        cls.data_out_storage_platform = os.environ["DATA_OUT_STORAGE_PLATFORM"]
        if cls.data_out_storage_platform not in StoragePlatform:
            raise RuntimeError(
                "Data out storage platform is invalid. Unable to set required configurations."
            )

        cls.app_root_dir = os.environ["APP_ROOT_DIR"]
        cls.global_cfg_dir = os.environ["GLOBAL_CFG_DIR"]

        # Host pattern specific env variables
        cls.log_handlers = json.loads(os.environ["LOG_HANDLERS"])
        cls.api_host = os.environ["API_HOST"]
        cls.api_port = os.environ["API_PORT"]
        if (
            cls.log_storage_platform == StoragePlatform.NAS_STORAGE
            or cls.data_in_storage_platform == StoragePlatform.NAS_STORAGE
            or cls.data_in_storage_platform == StoragePlatform.NAS_AWS_S3_STORAGE
            or cls.data_out_storage_platform == StoragePlatform.NAS_STORAGE
        ):
            cls.nas_root_dir = os.environ["NAS_ROOT_DIR"]
            cls.nas_log_dir = os.environ["NAS_LOG_DIR"]
            cls.nas_data_in_dir = os.environ["NAS_DATA_IN_DIR"]
            cls.nas_data_out_dir = os.environ["NAS_DATA_OUT_DIR"]
            cls.nas_img_out_dir = os.environ["NAS_IMG_OUT_DIR"]
            cls.nas_datalake_dir = os.environ["NAS_DATALAKE_DIR"]
        if (
            cls.log_storage_platform == StoragePlatform.AWS_S3_STORAGE
            or cls.data_in_storage_platform == StoragePlatform.AWS_S3_STORAGE
            or cls.data_in_storage_platform == StoragePlatform.NAS_AWS_S3_STORAGE
            or cls.data_out_storage_platform == StoragePlatform.AWS_S3_STORAGE
        ):
            cls.s3_log_bucket_uri = os.environ["S3_LOG_BUCKET_URI"]
            cls.s3_data_in_bucket_uri = os.environ["S3_DATA_IN_BUCKET_URI"]
            cls.s3_data_out_bucket_uri = os.environ["S3_DATA_OUT_BUCKET_URI"]
            cls.s3_img_out_bucket_uri = os.environ["S3_IMG_OUT_BUCKET_URI"]
            cls.s3_datalake_bucket_uri = os.environ["S3_DATALAKE_BUCKET_URI"]
            cls.s3_log_bucket, _ = ufa.parse_s3_uri(
                s3_obj_uri=os.environ["S3_LOG_BUCKET_URI"]
            )
            cls.s3_data_in_bucket, _ = ufa.parse_s3_uri(
                s3_obj_uri=os.environ["S3_DATA_IN_BUCKET_URI"]
            )
            cls.s3_data_out_bucket, _ = ufa.parse_s3_uri(
                s3_obj_uri=os.environ["S3_DATA_OUT_BUCKET_URI"]
            )
            cls.s3_img_out_bucket, _ = ufa.parse_s3_uri(
                s3_obj_uri=os.environ["S3_IMG_OUT_BUCKET_URI"]
            )
            cls.s3_datalake_bucket, _ = ufa.parse_s3_uri(
                s3_obj_uri=os.environ["S3_DATALAKE_BUCKET_URI"]
            )
            cls.s3_region = os.environ["S3_REGION"]

    @classmethod
    def set_config_file(cls):
        try:
            if cls.app_env in AppEnv:
                global_config_file = os.path.join(
                    cls.global_cfg_dir, f"global_config.{cls.app_env}.yaml"
                )
                app_config_file = os.path.join(
                    cls.app_root_dir, "cfg", f"app_config.{cls.app_env}.yaml"
                )
            else:
                raise RuntimeError(
                    "Environment is invalid. Expected values are prod / qa / dev ."
                )

            logging.info("Looking for global config file %s", global_config_file)
            if os.path.exists(global_config_file):
                logging.info("Reading global config file %s", global_config_file)
                cls.config.set_file(global_config_file)
            else:
                raise RuntimeError("Global config file is not found.")

            logging.info("Looking for app config file %s", app_config_file)
            if os.path.exists(app_config_file):
                logging.info("Reading app config file %s", app_config_file)
                cls.config.set_file(app_config_file)
            else:
                raise RuntimeError("App config file is not found.")

        except RuntimeError as error:
            logging.error(error)
            raise

    @classmethod
    def load_config(cls, app_host_pattern: str):
        cls.load_env_file(app_host_pattern=app_host_pattern)
        cls.set_env_vars()
        cls.set_config_file()

        global_cfg = cls.config["GLOBAL_CONFIG"].get()
        cls.app_config_dir = f"{cls.resolve_app_path(global_cfg['APP_CONFIG_DIR'])}"
        cls.app_sql_script_dir = (
            f"{cls.resolve_app_path(global_cfg['APP_SQL_SCRIPT_DIR'])}"
        )

        global_nas_cfg = cls.config["GLOBAL_CONFIG"]["NAS_STORAGE"].get()
        cls.app_log_dir = f"{cls.resolve_app_path(global_nas_cfg['APP_LOG_DIR'])}"
        cls.app_data_in_dir = (
            f"{cls.resolve_app_path(global_nas_cfg['APP_DATA_IN_DIR'])}"
        )
        cls.app_data_out_dir = (
            f"{cls.resolve_app_path(global_nas_cfg['APP_DATA_OUT_DIR'])}"
        )
        cls.app_img_out_dir = (
            f"{cls.resolve_app_path(global_nas_cfg['APP_IMG_OUT_DIR'])}"
        )
        cls.hive_warehouse_dir = (
            f"{cls.resolve_app_path(global_nas_cfg['HIVE_WAREHOUSE_DIR'])}"
        )
        if cls.log_storage_platform == StoragePlatform.NAS_STORAGE:
            cls.app_log_path = cls.app_log_dir
        if cls.data_out_storage_platform == StoragePlatform.NAS_STORAGE:
            cls.app_data_out_path = cls.app_data_out_dir
            cls.app_img_out_path = cls.app_img_out_dir
            cls.hive_warehouse_path = cls.hive_warehouse_dir

        global_aws_s3_cfg = cls.config["GLOBAL_CONFIG"]["AWS_S3_STORAGE"].get()
        cls.app_log_uri = f"{cls.resolve_app_path(global_aws_s3_cfg['APP_LOG_URI'])}"
        cls.app_data_in_uri = (
            f"{cls.resolve_app_path(global_aws_s3_cfg['APP_DATA_IN_URI'])}"
        )
        cls.app_data_out_uri = (
            f"{cls.resolve_app_path(global_aws_s3_cfg['APP_DATA_OUT_URI'])}"
        )
        cls.app_img_out_uri = (
            f"{cls.resolve_app_path(global_aws_s3_cfg['APP_IMG_OUT_URI'])}"
        )
        cls.hive_warehouse_uri = (
            f"{cls.resolve_app_path(global_aws_s3_cfg['HIVE_WAREHOUSE_URI'])}"
        )
        if cls.log_storage_platform == StoragePlatform.AWS_S3_STORAGE:
            cls.app_log_path = cls.app_log_uri
        if cls.data_out_storage_platform == StoragePlatform.AWS_S3_STORAGE:
            cls.app_data_out_path = cls.app_data_out_uri
            cls.app_img_out_path = cls.app_img_out_uri
            cls.hive_warehouse_path = cls.hive_warehouse_uri

        app_cfg = cls.config["APP_CONFIG"].get()
        cls.app_name = app_cfg["APP_NAME"]

    @classmethod
    def load_aws_config(cls, aws_iam_user_name: str):
        cls.set_env_vars()
        cls.set_config_file()

        if user := aws_iam_user_name.upper():
            aws_user_cfg = cls.config["GLOBAL_CONFIG"]["AWS_S3_STORAGE"][
                "AWS_USER_CONFIG"
            ][user].get()

            cls.s3_prefix = aws_user_cfg["S3_PREFIX"]
            cls.s3_bucket_uri = aws_user_cfg["S3_BUCKET_URI"]
            cls.s3_region = aws_user_cfg["S3_REGION"]

    @classmethod
    def resolve_app_path(cls, rel_path):
        global_cfg = cls.config["GLOBAL_CONFIG"].get()
        global_nas_cfg = cls.config["GLOBAL_CONFIG"]["NAS_STORAGE"].get()
        global_aws_s3_cfg = cls.config["GLOBAL_CONFIG"]["AWS_S3_STORAGE"].get()
        app_cfg = cls.config["APP_CONFIG"].get()
        resolved_app_path = (
            rel_path.replace(  # metadata to cfg map start
                "APP_CONFIG_DIR", global_cfg["APP_CONFIG_DIR"]
            )
            .replace("APP_SQL_SCRIPT_DIR", global_cfg["APP_SQL_SCRIPT_DIR"])
            .replace("APP_LOG_DIR", global_nas_cfg["APP_LOG_DIR"])
            .replace("APP_DATA_IN_DIR", global_nas_cfg["APP_DATA_IN_DIR"])
            .replace("APP_DATA_OUT_DIR", global_nas_cfg["APP_DATA_OUT_DIR"])
            .replace("APP_IMG_OUT_DIR", global_nas_cfg["APP_IMG_OUT_DIR"])
            .replace("HIVE_WAREHOUSE_DIR", global_nas_cfg["HIVE_WAREHOUSE_DIR"])
            .replace("APP_LOG_URI", global_aws_s3_cfg["APP_LOG_URI"])
            .replace("APP_DATA_IN_URI", global_aws_s3_cfg["APP_DATA_IN_URI"])
            .replace("APP_DATA_OUT_URI", global_aws_s3_cfg["APP_DATA_OUT_URI"])
            .replace("APP_IMG_OUT_URI", global_aws_s3_cfg["APP_IMG_OUT_URI"])
            .replace("HIVE_WAREHOUSE_URI", global_aws_s3_cfg["HIVE_WAREHOUSE_URI"])
            .replace("APP_NAME", app_cfg["APP_NAME"])
            # metadata to cfg map end
            .replace("NAS_LOG_DIR", cls.nas_log_dir)
            .replace("NAS_DATA_IN_DIR", cls.nas_data_in_dir)
            .replace("NAS_DATA_OUT_DIR", cls.nas_data_out_dir)
            .replace("NAS_IMG_OUT_DIR", cls.nas_img_out_dir)
            .replace("S3_LOG_BUCKET_URI", cls.s3_log_bucket_uri)
            .replace("S3_DATA_IN_BUCKET_URI", cls.s3_data_in_bucket_uri)
            .replace("S3_DATA_OUT_BUCKET_URI", cls.s3_data_out_bucket_uri)
            .replace("S3_IMG_OUT_BUCKET_URI", cls.s3_img_out_bucket_uri)
            .replace("APP_ROOT_DIR", cls.app_root_dir)
            .replace("NAS_ROOT_DIR", cls.nas_root_dir)
            .replace("APP_NAME", cls.app_name)
        )

        return resolved_app_path
