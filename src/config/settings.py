import confuse
from dotenv import load_dotenv
import os
import logging

# APP_ROOT_DIR = "/workspaces/df-config"
# Fail if APP_ROOT_DIR env variable is not set
APP_ROOT_DIR = os.environ['APP_ROOT_DIR']


class ConfigParms:
    config = confuse.Configuration("dist_app", __name__)

    # Define config variables at module scope
    cfg_file_path = ""
    log_file_path = ""
    data_in_file_path = ""
    data_out_file_path = ""
    sql_script_file_path = ""
    img_out_file_path = ""
    hive_warehouse_path = ""

    @classmethod
    def load_config(cls, env: str):
        # Load the environment variables from .env file
        load_dotenv()
        logging.info(os.environ)

        try:
            if env == "prod":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config.yaml")
            elif env == "qa":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config_qa.yaml")
            elif env == "dev":
                cls.config.set_file(f"{APP_ROOT_DIR}/cfg/config_dev.yaml")
            else:
                raise ValueError(
                    "Environment is invalid. Accepted values are prod / qa / dev ."
                )
        except ValueError as error:
            logging.error(error)
            raise

        cfg = cls.config["CONFIG"].get()
        logging.info(cfg)

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
            .replace("APP_ROOT_DIR", APP_ROOT_DIR)
        )
        return resolved_app_path
