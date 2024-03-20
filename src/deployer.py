import os
import sys
import logging
from datetime import datetime, timezone
import json
import zipfile
from base64 import b64decode
import sesamclient


# Define constants for environment variable names
INPUT_NODE = "INPUT_NODE"
INPUT_JWT = "INPUT_JWT"
INPUT_DRY_RUN = "INPUT_DRY_RUN"
INPUT_FORCE_CONFIG = "INPUT_FORCE_CONFIG"
INPUT_REPLACE_SECRETS = "INPUT_REPLACE_SECRETS"
INPUT_SECRETS_FILE = "INPUT_SECRETS_FILE"
INPUT_VARIABLES_FILE = "INPUT_VARIABLES_FILE"
INPUT_USE_WHITELIST = "INPUT_USE_WHITELIST"
INPUT_CONFIG_FOLDER = "INPUT_CONFIG_FOLDER"
INPUT_WRITE_SUMMARY = "INPUT_WRITE_SUMMARY"

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a logger instance with the name "deployer"
logger = logging.getLogger("sesam")


class SummaryHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)

    def emit(self, record):
        try:
            msg = self.format(record)
            # Get the log level name
            log_level = logging.getLevelName(record.levelno)
            # Get the timestamp with timezone
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # Write formatted log message with timestamp and log level to $GITHUB_STEP_SUMMARY file if available
            if "GITHUB_STEP_SUMMARY" in os.environ:
                with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary_file:
                    summary_file.write(f"{timestamp} {log_level}: {msg}\n")
        except Exception as e:
            self.handleError(record)


class SesamNode:
    """Sesam node functions wrapped in a class"""

    def __init__(self, node_url, jwt_token, logger):
        self.logger = logger
        self.node_url = node_url
        self.jwt_token = jwt_token
        self.subscription_id = None

        # Pull data chunk from the jwt token
        _, payload, _ = self.jwt_token.split(".")
        # Add padding to base64 and decode it
        jwt_data = json.loads(b64decode(payload + "=="))

        # Extract the sub ID from the data
        if jwt_data:
            principals = jwt_data.get("principals", {})
            subscriptions = list(principals.keys())
            if len(subscriptions) > 0:
                self.subscription_id = subscriptions[0]

        safe_jwt = "{}*********{}".format(jwt_token[:10], jwt_token[-10:])
        self.logger.info(f"Connecting to Sesam using url {node_url}' and JWT {safe_jwt}")
        self.logger.info(f"JWT Principals {principals}")

        self.api_connection = sesamclient.Connection(
            sesamapi_base_url=self.node_url,
            jwt_auth_token=self.jwt_token,
            timeout=60 * 10,
        )
    def get_health(self):
        self.logger.info(f"GET health from {self.node_url}")
        return self.api_connection.get_health()
    
    def put_env(self, env_vars):
        self.logger.info(f"PUT env vars to {self.node_url}")
        return self.api_connection.put_env_vars(env_vars)

    def post_secrets(self, secrets):
        self.logger.info(f"POST secrets to {self.node_url}")
        return self.api_connection.post_secrets(secrets)

    def put_secrets(self, secrets):
        self.logger.info(f"PUT secrets to {self.node_url}")
        return self.api_connection.put_secrets(secrets)
    
    def put_config(self, config, force=False):
        self.logger.info(f"PUT config to {self.node_url}")
        return self.api_connection.upload_config(config, force=force)
    

def check_required_env_vars(required_vars):
    missing_vars = [var for var in required_vars if os.environ.get(var) is None]

    if missing_vars:
        for var in missing_vars:
            logger.error(f"Required environment variable '{var}' is missing.")
        return False
    else:
        return True
    
    
def parse_bool_env(env_value, default=False):
    if env_value is not None:
        env_value = env_value.lower()  # Convert to lowercase for case-insensitive comparison
        if env_value in {"true", "1", "yes"}:
            return True
        elif env_value in {"false", "0", "no"}:
            return False
    return default


def create_zipped_config(logger, input_folder, output_zip, whitelist=False):
    try:
        with zipfile.ZipFile(output_zip, 'w') as zipf:
            # Whitelist mode
            whitelist_files = set()
            if whitelist:
                whitelist_path = os.path.join(input_folder, 'deployment', 'whitelist.txt')
                logger.info(f"Creating config using {whitelist_path}")
                if os.path.exists(whitelist_path):
                    with open(whitelist_path) as whitelist_file:
                        for line in whitelist_file:
                            line = line.strip()
                            if os.path.exists(os.path.join(input_folder, line)):
                                whitelist_files.add(line)

            # Add node-metadata.conf.json if exists
            node_metadata_path = os.path.join(input_folder, 'node-metadata.conf.json')
            if os.path.exists(node_metadata_path) and (not whitelist or 'node-metadata.conf.json' in whitelist_files):
                zipf.write(node_metadata_path, 'node-metadata.conf.json')
                logger.info(f"Added file: {node_metadata_path}")

            # Counter to track the number of files added
            files_added_count = 0

            # Traverse directories
            for root, dirs, files in os.walk(input_folder):
                if os.path.basename(root) in ['pipes', 'systems']:
                    for file in files:
                        file_path = os.path.join(root, file)
                        if whitelist:
                            rel_file_path = os.path.relpath(file_path, input_folder)
                            if rel_file_path not in whitelist_files:
                                continue
                        if file.endswith('conf.json'):
                            zipf.write(file_path, os.path.relpath(file_path, input_folder))
                            logger.info(f"Added file: {file_path}")
                            files_added_count += 1
            
            # Log number of files were added
            logger.info(f"{files_added_count} files were added to the zip file.")
    except (FileNotFoundError, PermissionError, IOError) as e:
        logger.error(f"An error occurred while creating the zip file: {str(e)}")
        return None

    return output_zip


def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON in file '{file_path}': {str(e)}")
    

def deploy_secrets(sesam_node, secrets_file, dry_run, replace_secrets):
    if secrets_file:
        logger.info(f"=> deploying secrets using file: {secrets_file}")
        logger.info(f"dry_run: {dry_run}")
        logger.info(f"replace_secrets: {replace_secrets}")
        secrets_json = read_json_file(secrets_file)

        if dry_run:
            logger.info("result: dry run enabled. No secrets deployed to the Sesam node.")
            return

        if replace_secrets:
            deploy_secrets = sesam_node.put_secrets(secrets_json)
        else:
            deploy_secrets = sesam_node.post_secrets(secrets_json)
 
        logger.info(f"result: {deploy_secrets}")

def deploy_variables(sesam_node, variables_file, dry_run):
    if variables_file:
        logger.info(f"=> deploying variables using file: {variables_file}")
        logger.info(f"dry_run: {dry_run}")
        variables_json = read_json_file(variables_file)

        if dry_run:
            logger.info("result: dry run enabled. No variables deployed to the Sesam node.")
            return

        deploy_variables = sesam_node.put_env(variables_json)
        logger.info(f"result: {deploy_variables}")

def deploy_config(sesam_node, config_folder, dry_run, use_whitelist, force_config):
    if config_folder:
        logger.info(f"=> deploying config using folder: {config_folder}")
        logger.info(f"dry_run: {dry_run}")
        logger.info(f"use_whitelist: {use_whitelist}")
        logger.info(f"force_config: {force_config}")

        output_zip = 'config.zip'
        zip_path = create_zipped_config(logger, config_folder, output_zip, use_whitelist)

        if dry_run:
            logger.info("result: dry run enabled. No config deployed to the Sesam node.")
            return

        with open(zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        deploy_config = sesam_node.put_config(zip_content, force=force_config)
        logger.info(f"result: {deploy_config}")

def main():
    try:
        # check required env vars
        required_env_vars = ["INPUT_NODE", "INPUT_JWT", "INPUT_CONFIG_FOLDER"]

        if not check_required_env_vars(required_env_vars):
            sys.exit(1)

        # Get environment variables
        node = os.environ.get(INPUT_NODE)
        jwt_token = os.environ.get(INPUT_JWT)
        config_folder = os.environ.get(INPUT_CONFIG_FOLDER)
        force_config = parse_bool_env(os.environ.get(INPUT_FORCE_CONFIG, "False"))
        secrets_file = os.environ.get(INPUT_SECRETS_FILE)
        replace_secrets = parse_bool_env(os.environ.get(INPUT_REPLACE_SECRETS, "False"))
        variables_file = os.environ.get(INPUT_VARIABLES_FILE)
        use_whitelist = parse_bool_env(os.environ.get(INPUT_USE_WHITELIST, "False"))
        dry_run = parse_bool_env(os.environ.get(INPUT_DRY_RUN, "True"))
        write_summary = parse_bool_env(os.environ.get(INPUT_WRITE_SUMMARY, "False"))

        # Add the custom handler if write_summary is enabled
        if write_summary:
            summary_handler = SummaryHandler()
            logger.addHandler(summary_handler)

        # configure the sesam_node_url
        sesam_node_url = f'https://{node}/api'

        # Instantiate the SesamNode class
        sesam_node = SesamNode(sesam_node_url, jwt_token, logger)

        # check the node health
        node_health = sesam_node.get_health()
        if node_health.get('status') == 'ok':
            logger.info(f"node_status: {node_health.get('status').upper()} (uptime: {node_health.get('node_uptime')})")
        else:
            logger.error("Node health status not OK. Program will be exited.")
            exit(1)

        # Deploy secrets, varables and confg
        deploy_secrets(sesam_node, secrets_file, dry_run, replace_secrets)
        deploy_variables(sesam_node, variables_file, dry_run)
        deploy_config(sesam_node, config_folder, dry_run, use_whitelist, force_config)

    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()