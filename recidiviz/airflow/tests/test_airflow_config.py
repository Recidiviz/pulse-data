# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Template for configuration used across Airflow integration tests"""

# pylint: disable=W1401
AIRFLOW_INTEGRATION_TEST_CONFIG_TEMPLATE = """
[core]
hostname_callable = airflow.utils.net.getfqdn
default_timezone = utc
executor = SequentialExecutor
parallelism = 32
max_active_tasks_per_dag = 16
dags_are_paused_at_creation = True
max_active_runs_per_dag = 16
load_examples = True
plugins_folder = {data_dir}/airflow/plugins
execute_tasks_new_python_interpreter = False
fernet_key =
donot_pickle = True
dagbag_import_timeout = 30.0
dagbag_import_error_tracebacks = True
dagbag_import_error_traceback_depth = 2
dag_file_processor_timeout = 50
task_runner = StandardTaskRunner
default_impersonation =
security =
enable_xcom_pickling = False
allowed_deserialization_classes = airflow\..*
killed_task_cleanup_time = 60
dag_run_conf_overrides_params = True
dag_discovery_safe_mode = True
dag_ignore_file_syntax = regexp
default_task_retries = 0
default_task_retry_delay = 300
default_task_weight_rule = downstream
default_task_execution_timeout =
min_serialized_dag_update_interval = 30
compress_serialized_dags = False
min_serialized_dag_fetch_interval = 10
max_num_rendered_ti_fields_per_task = 30
check_slas = True
xcom_backend = airflow.models.xcom.BaseXCom
lazy_load_plugins = True
lazy_discover_providers = True
hide_sensitive_var_conn_fields = True
sensitive_var_conn_names =
default_pool_task_slot_count = 128
max_map_length = 1024
daemon_umask = 0o077
unit_test_mode = False

[logging]
logging_level = DEBUG
fab_logging_level = DEBUG
log_format = [%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s
dag_processor_log_format = [%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s
colored_log_format = [%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s
colored_console_log = False
colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter
dag_processor_log_target = file
base_log_folder = {data_dir}/logs
dag_processor_manager_log_location = {data_dir}/logs
log_processor_filename_template = {{{{ filename }}}}.log
remote_logging = False

[scheduler]
child_process_log_directory = {data_dir}/logs
job_heartbeat_sec = 5
scheduler_heartbeat_sec = 5
num_runs = -1
scheduler_idle_sleep_time = 1
min_file_process_interval = 30
parsing_cleanup_interval = 60
dag_dir_list_interval = 300
print_stats_interval = 30
pool_metrics_interval = 5.0
scheduler_health_check_threshold = 30
enable_health_check = False
scheduler_health_check_server_port = 8974
orphaned_tasks_check_interval = 300.0
scheduler_zombie_task_threshold = 300
zombie_detection_interval = 10.0
catchup_by_default = True
ignore_first_depends_on_past_by_default = True
max_tis_per_query = 512
use_row_level_locking = True
max_dagruns_to_create_per_loop = 10
max_dagruns_per_loop_to_schedule = 20
schedule_after_task_execution = True
parsing_processes = 2
file_parsing_sort_mode = modified_time
standalone_dag_processor = False
max_callbacks_per_loop = 20
dag_stale_not_seen_duration = 600
use_job_schedule = True
allow_trigger_in_future = False
trigger_timeout_check_interval = 15

[webserver]
base_url = http://localhost:8080
default_ui_timezone = UTC
web_server_host = 0.0.0.0
web_server_port = 8080
web_server_ssl_cert =
web_server_ssl_key =
session_backend = database
web_server_master_timeout = 120
web_server_worker_timeout = 120
worker_refresh_batch_size = 1
worker_refresh_interval = 6000
reload_on_plugin_change = False
secret_key = qKNc0hB3pUp9DeSzHxtHjQ==
workers = 4
worker_class = sync
access_logfile = -
error_logfile = -
access_logformat =
expose_config = False
expose_hostname = True
expose_stacktrace = False
dag_default_view = grid
dag_orientation = LR
log_fetch_timeout_sec = 5
log_fetch_delay_sec = 2
log_auto_tailing_offset = 30
log_animation_speed = 1000
hide_paused_dags_by_default = False
page_size = 100
navbar_color = #fff
default_dag_run_display_number = 25
enable_proxy_fix = False
proxy_fix_x_for = 1
proxy_fix_x_proto = 1
proxy_fix_x_host = 1
proxy_fix_x_port = 1
proxy_fix_x_prefix = 1
cookie_secure = False
cookie_samesite = Lax
default_wrap = False
x_frame_enabled = True
show_recent_stats_for_completed_runs = True
update_fab_perms = True
session_lifetime_minutes = 43200
instance_name_has_markup = False
auto_refresh_interval = 3
warn_deployment_exposure = True
audit_view_excluded_events = gantt,landing_times,tries,duration,calendar,graph,grid,tree,tree_data
"""
